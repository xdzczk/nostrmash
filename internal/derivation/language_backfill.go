package derivation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	languageBackfillName         = "discovery_language"
	languageBackfillNoteBatch    = 200
	languageBackfillProfileBatch = 100

	// LanguageBackfillPhaseNotes reclassifies notes in the stored window.
	LanguageBackfillPhaseNotes = "notes"
	// LanguageBackfillPhaseProfiles fills profile language columns from those notes.
	LanguageBackfillPhaseProfiles = "profiles"
	// LanguageBackfillPhaseMeili waits for the worker to resync the search index.
	LanguageBackfillPhaseMeili = "meili"
	// LanguageBackfillPhaseDone is a finished backfill.
	LanguageBackfillPhaseDone = "done"

	languageBackfillStatusRunning   = "running"
	languageBackfillStatusSucceeded = "succeeded"
)

// LanguageBackfillStatus is the cursor row for the 7-day language backfill.
type LanguageBackfillStatus struct {
	Name            string     `json:"name"`
	Phase           string     `json:"phase"`
	Status          string     `json:"status"`
	WindowStart     int64      `json:"window_start"`
	CursorCreatedAt int64      `json:"cursor_created_at"`
	CursorEventID   string     `json:"cursor_event_id"`
	CursorPubkey    string     `json:"cursor_pubkey"`
	Scanned         int64      `json:"scanned"`
	Updated         int64      `json:"updated"`
	LastError       string     `json:"last_error,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

// LanguageBackfillAutostart reports whether the worker should insert a
// backfill cursor on startup when one does not already exist.
func LanguageBackfillAutostart() bool {
	return getEnvBoolOrDefault("NOSTRMASH_LANGUAGE_BACKFILL", false)
}

// EnsureLanguageBackfill inserts a notes-phase cursor when the table has no
// row. An existing run, including a finished one, is left alone.
func (h *Handlers) EnsureLanguageBackfill(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	windowStart := time.Now().UTC().Add(-7 * 24 * time.Hour).Unix()
	_, err := h.pool.Exec(ctx, `
		INSERT INTO derivation_backfill_cursors (
			name, phase, status, window_start
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name) DO NOTHING
	`, languageBackfillName, LanguageBackfillPhaseNotes, languageBackfillStatusRunning, windowStart)
	if err != nil {
		return fmt.Errorf("ensure language backfill: %w", err)
	}
	return nil
}

// StartLanguageBackfill resets the cursor to the notes phase unless a run
// is already moving. A cursor quiet for 30 minutes is treated as stalled
// and restarted from the beginning of the window.
func (h *Handlers) StartLanguageBackfill(ctx context.Context) (LanguageBackfillStatus, error) {
	if h == nil || h.pool == nil {
		return LanguageBackfillStatus{}, fmt.Errorf("handlers are not initialized")
	}
	windowStart := time.Now().UTC().Add(-7 * 24 * time.Hour).Unix()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("begin language backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, ok, err := loadLanguageBackfillStatus(ctx, tx)
	if err != nil {
		return LanguageBackfillStatus{}, err
	}
	if ok && current.Status == languageBackfillStatusRunning && current.Phase != LanguageBackfillPhaseDone && time.Since(current.UpdatedAt) < 30*time.Minute {
		if err := tx.Commit(ctx); err != nil {
			return LanguageBackfillStatus{}, fmt.Errorf("commit language backfill status: %w", err)
		}
		return current, nil
	}
	row, err := scanLanguageBackfillStatus(tx.QueryRow(ctx, `
		INSERT INTO derivation_backfill_cursors (
			name, phase, status, window_start,
			cursor_created_at, cursor_event_id, cursor_pubkey,
			scanned, updated_rows, last_error, updated_at, finished_at
		)
		VALUES ($1, $2, $3, $4, 0, '', '', 0, 0, NULL, now(), NULL)
		ON CONFLICT (name) DO UPDATE
		SET phase = EXCLUDED.phase,
		    status = EXCLUDED.status,
		    window_start = EXCLUDED.window_start,
		    cursor_created_at = 0,
		    cursor_event_id = '',
		    cursor_pubkey = '',
		    scanned = 0,
		    updated_rows = 0,
		    last_error = NULL,
		    updated_at = now(),
		    finished_at = NULL
		RETURNING name, phase, status, window_start, cursor_created_at, cursor_event_id,
		          cursor_pubkey, scanned, updated_rows, last_error, updated_at, finished_at
	`, languageBackfillName, LanguageBackfillPhaseNotes, languageBackfillStatusRunning, windowStart))
	if err != nil {
		return LanguageBackfillStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("commit language backfill start: %w", err)
	}
	return row, nil
}

// LanguageBackfillStatus reads the cursor. ok is false when no run exists.
func (h *Handlers) LanguageBackfillStatus(ctx context.Context) (LanguageBackfillStatus, bool, error) {
	if h == nil || h.pool == nil {
		return LanguageBackfillStatus{}, false, fmt.Errorf("handlers are not initialized")
	}
	return loadLanguageBackfillStatus(ctx, h.pool)
}

// RunLanguageBackfillBatch advances one notes or profiles batch. A meili
// or done cursor is returned unchanged so the worker can run search sync.
func (h *Handlers) RunLanguageBackfillBatch(ctx context.Context) (LanguageBackfillStatus, error) {
	if h == nil || h.pool == nil {
		return LanguageBackfillStatus{}, fmt.Errorf("handlers are not initialized")
	}
	current, ok, err := loadLanguageBackfillStatus(ctx, h.pool)
	if err != nil {
		return LanguageBackfillStatus{}, err
	}
	if !ok || current.Phase == LanguageBackfillPhaseMeili || current.Phase == LanguageBackfillPhaseDone {
		return current, nil
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("begin language backfill batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', '60s', true)`); err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("set language backfill timeout: %w", err)
	}

	var next LanguageBackfillStatus
	switch current.Phase {
	case LanguageBackfillPhaseProfiles:
		next, err = h.runProfileLanguageBatch(ctx, tx, current)
	default:
		next, err = h.runNoteLanguageBatch(ctx, tx, current)
	}
	if err != nil {
		return LanguageBackfillStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("commit language backfill batch: %w", err)
	}
	return next, nil
}

// NoteLanguageBackfillError records a search-sync failure without moving
// the cursor backwards.
func (h *Handlers) NoteLanguageBackfillError(ctx context.Context, cause error) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := h.pool.Exec(ctx, `
		UPDATE derivation_backfill_cursors
		SET last_error = $2, updated_at = now()
		WHERE name = $1
	`, languageBackfillName, message)
	if err != nil {
		return fmt.Errorf("record language backfill error: %w", err)
	}
	return nil
}

// FinishLanguageBackfill marks the search sync complete.
func (h *Handlers) FinishLanguageBackfill(ctx context.Context) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	_, err := h.pool.Exec(ctx, `
		UPDATE derivation_backfill_cursors
		SET phase = $2,
		    status = $3,
		    last_error = NULL,
		    finished_at = now(),
		    updated_at = now()
		WHERE name = $1
	`, languageBackfillName, LanguageBackfillPhaseDone, languageBackfillStatusSucceeded)
	if err != nil {
		return fmt.Errorf("finish language backfill: %w", err)
	}
	return nil
}

type languageBackfillQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func loadLanguageBackfillStatus(ctx context.Context, q languageBackfillQuerier) (LanguageBackfillStatus, bool, error) {
	row, err := scanLanguageBackfillStatus(q.QueryRow(ctx, `
		SELECT name, phase, status, window_start, cursor_created_at, cursor_event_id,
		       cursor_pubkey, scanned, updated_rows, last_error, updated_at, finished_at
		FROM derivation_backfill_cursors
		WHERE name = $1
	`, languageBackfillName))
	if errors.Is(err, pgx.ErrNoRows) {
		return LanguageBackfillStatus{}, false, nil
	}
	if err != nil {
		return LanguageBackfillStatus{}, false, err
	}
	return row, true, nil
}

func scanLanguageBackfillStatus(row pgx.Row) (LanguageBackfillStatus, error) {
	var out LanguageBackfillStatus
	var lastError *string
	if err := row.Scan(
		&out.Name,
		&out.Phase,
		&out.Status,
		&out.WindowStart,
		&out.CursorCreatedAt,
		&out.CursorEventID,
		&out.CursorPubkey,
		&out.Scanned,
		&out.Updated,
		&lastError,
		&out.UpdatedAt,
		&out.FinishedAt,
	); err != nil {
		return LanguageBackfillStatus{}, err
	}
	if lastError != nil {
		out.LastError = *lastError
	}
	return out, nil
}

type noteLanguageCandidate struct {
	eventID    string
	author     string
	createdAt  int64
	content    string
	language   *string
	confidence *float64
	version    *int32
	source     *string
}

func (h *Handlers) runNoteLanguageBatch(ctx context.Context, tx pgx.Tx, current LanguageBackfillStatus) (LanguageBackfillStatus, error) {
	rows, err := tx.Query(ctx, `
		SELECT
			s.event_id,
			e.pubkey,
			s.created_at,
			COALESCE(e.content, ''),
			s.primary_language,
			s.language_confidence,
			s.derivation_version,
			s.language_source
		FROM note_discovery_stats s
		JOIN events e ON e.id = s.event_id
		WHERE s.created_at >= $1
		  AND (s.language_source IS NULL OR s.language_source IN ('short', 'uncertain'))
		  AND (s.created_at, s.event_id) > ($2, $3)
		ORDER BY s.created_at, s.event_id
		LIMIT $4
	`, current.WindowStart, current.CursorCreatedAt, current.CursorEventID, languageBackfillNoteBatch)
	if err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("list note language batch: %w", err)
	}
	defer rows.Close()
	batch := make([]noteLanguageCandidate, 0, languageBackfillNoteBatch)
	for rows.Next() {
		var row noteLanguageCandidate
		if err := rows.Scan(&row.eventID, &row.author, &row.createdAt, &row.content, &row.language, &row.confidence, &row.version, &row.source); err != nil {
			return LanguageBackfillStatus{}, fmt.Errorf("scan note language batch: %w", err)
		}
		batch = append(batch, row)
	}
	if err := rows.Err(); err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("read note language batch: %w", err)
	}
	if len(batch) == 0 {
		return saveLanguageBackfillCursor(ctx, tx, current, LanguageBackfillPhaseProfiles, "", 0, "", 0, 0)
	}

	nowUnix := time.Now().UTC().Unix()
	updated := int64(0)
	for _, row := range batch {
		lang, conf, source, err := resolveNoteLanguageTx(ctx, tx, row.eventID, row.author, row.content, nowUnix, row.version, row.language, row.confidence, row.source)
		if err != nil {
			return LanguageBackfillStatus{}, err
		}
		if source == "" {
			continue
		}
		tag, err := tx.Exec(ctx, `
			UPDATE note_discovery_stats
			SET primary_language = $2,
			    language_confidence = $3,
			    language_source = $4,
			    projected_at = now()
			WHERE event_id = $1
			  AND (
				primary_language IS DISTINCT FROM $2
				OR language_confidence IS DISTINCT FROM $3
				OR language_source IS DISTINCT FROM $4
			  )
		`, row.eventID, lang, conf, source)
		if err != nil {
			return LanguageBackfillStatus{}, fmt.Errorf("update note language %s: %w", row.eventID, err)
		}
		updated += tag.RowsAffected()
	}
	last := batch[len(batch)-1]
	return saveLanguageBackfillCursor(ctx, tx, current, LanguageBackfillPhaseNotes, "", last.createdAt, last.eventID, int64(len(batch)), updated)
}

func (h *Handlers) runProfileLanguageBatch(ctx context.Context, tx pgx.Tx, current LanguageBackfillStatus) (LanguageBackfillStatus, error) {
	rows, err := tx.Query(ctx, `
		SELECT pubkey
		FROM profile_discovery_stats
		WHERE recent_activity_at IS NOT NULL
		  AND recent_activity_at >= $1
		  AND pubkey > $2
		ORDER BY pubkey
		LIMIT $3
	`, current.WindowStart, current.CursorPubkey, languageBackfillProfileBatch)
	if err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("list profile language batch: %w", err)
	}
	defer rows.Close()
	pubkeys := make([]string, 0, languageBackfillProfileBatch)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return LanguageBackfillStatus{}, fmt.Errorf("scan profile language batch: %w", err)
		}
		pubkeys = append(pubkeys, pubkey)
	}
	if err := rows.Err(); err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("read profile language batch: %w", err)
	}
	if len(pubkeys) == 0 {
		return saveLanguageBackfillCursor(ctx, tx, current, LanguageBackfillPhaseMeili, "", current.CursorCreatedAt, current.CursorEventID, 0, 0)
	}

	nowUnix := time.Now().UTC().Unix()
	updated := int64(0)
	for _, pubkey := range pubkeys {
		lang24, err := lookupWindowDominantLanguage(ctx, tx, pubkey, nowUnix-int64((24*time.Hour)/time.Second))
		if err != nil {
			return LanguageBackfillStatus{}, err
		}
		lang7, err := lookupWindowDominantLanguage(ctx, tx, pubkey, nowUnix-int64((7*24*time.Hour)/time.Second))
		if err != nil {
			return LanguageBackfillStatus{}, err
		}
		tag, err := tx.Exec(ctx, `
			UPDATE profile_discovery_stats
			SET primary_language_24h = $2,
			    primary_language_7d = $3
			WHERE pubkey = $1
			  AND (
				primary_language_24h IS DISTINCT FROM $2
				OR primary_language_7d IS DISTINCT FROM $3
			  )
		`, pubkey, lang24, lang7)
		if err != nil {
			return LanguageBackfillStatus{}, fmt.Errorf("update profile language %s: %w", pubkey, err)
		}
		updated += tag.RowsAffected()
	}
	return saveLanguageBackfillCursor(ctx, tx, current, LanguageBackfillPhaseProfiles, pubkeys[len(pubkeys)-1], current.CursorCreatedAt, current.CursorEventID, int64(len(pubkeys)), updated)
}

func saveLanguageBackfillCursor(
	ctx context.Context,
	tx pgx.Tx,
	current LanguageBackfillStatus,
	phase string,
	pubkey string,
	createdAt int64,
	eventID string,
	scanned int64,
	updated int64,
) (LanguageBackfillStatus, error) {
	if pubkey == "" {
		pubkey = current.CursorPubkey
	}
	row, err := scanLanguageBackfillStatus(tx.QueryRow(ctx, `
		UPDATE derivation_backfill_cursors
		SET phase = $2,
		    status = $3,
		    cursor_created_at = $4,
		    cursor_event_id = $5,
		    cursor_pubkey = $6,
		    scanned = scanned + $7,
		    updated_rows = updated_rows + $8,
		    last_error = NULL,
		    updated_at = now()
		WHERE name = $1
		RETURNING name, phase, status, window_start, cursor_created_at, cursor_event_id,
		          cursor_pubkey, scanned, updated_rows, last_error, updated_at, finished_at
	`, languageBackfillName, phase, languageBackfillStatusRunning, createdAt, eventID, pubkey, scanned, updated))
	if err != nil {
		return LanguageBackfillStatus{}, fmt.Errorf("save language backfill cursor: %w", err)
	}
	return row, nil
}

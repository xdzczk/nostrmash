package api_primal

import (
	"encoding/json"
	"errors"
	"strings"
)

func toStringSlice(v any) []string {
	values, ok := v.([]any)
	if !ok {
		if stringsValue, ok := v.([]string); ok {
			return stringsValue
		}
		return []string{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		s, ok := value.(string)
		if !ok {
			continue
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func toInt(v any, fallback int) int {
	switch value := v.(type) {
	case int:
		if value > 0 {
			return value
		}
	case float64:
		casted := int(value)
		if casted > 0 {
			return casted
		}
	}
	return fallback
}

func toBoundedPositiveInt(v any, fallback int, max int) int {
	value := toInt(v, fallback)
	if value <= 0 {
		return fallback
	}
	if value > max {
		return max
	}
	return value
}

func toBoundedNonNegativeInt(v any, fallback int, max int) int {
	value := fallback
	switch typed := v.(type) {
	case int:
		value = typed
	case float64:
		value = int(typed)
	}
	if value < 0 {
		value = 0
	}
	if value > max {
		value = max
	}
	return value
}

func compatIdentifierValue(kwargs map[string]any) (string, bool, error) {
	if value, ok := kwargs["identifier"]; ok {
		identifier, ok := value.(string)
		if !ok {
			return "", false, errors.New("identifier is not a string")
		}
		return strings.TrimSpace(identifier), true, nil
	}
	if value, ok := kwargs["d_tag"]; ok {
		identifier, ok := value.(string)
		if !ok {
			return "", false, errors.New("d_tag is not a string")
		}
		return strings.TrimSpace(identifier), true, nil
	}
	return "", false, nil
}

type parameterizedReplaceableRef struct {
	pubkey     string
	kind       int
	identifier string
}

func parseParameterizedReplaceableRefs(raw any) ([]parameterizedReplaceableRef, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, errors.New("events must be an array")
	}
	out := make([]parameterizedReplaceableRef, 0, len(values))
	for _, value := range values {
		entry, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("event entry must be an object")
		}
		pubkey := strings.TrimSpace(stringValue(entry["pubkey"]))
		kind := toInt(entry["kind"], 0)
		identifier, hasIdentifier, err := compatIdentifierValue(entry)
		if err != nil {
			return nil, err
		}
		if pubkey == "" || kind <= 0 || !hasIdentifier {
			return nil, errors.New("event entry must include pubkey, kind and identifier")
		}
		out = append(out, parameterizedReplaceableRef{
			pubkey:     pubkey,
			kind:       kind,
			identifier: identifier,
		})
	}
	return out, nil
}

func optionalStringValue(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	value, ok := v.(string)
	if !ok {
		return "", errors.New("value is not a string")
	}
	return strings.TrimSpace(value), nil
}

func toInt64(v any, fallback int64) int64 {
	switch value := v.(type) {
	case int:
		if value >= 0 {
			return int64(value)
		}
	case int64:
		if value >= 0 {
			return value
		}
	case float64:
		casted := int64(value)
		if casted >= 0 {
			return casted
		}
	}
	return fallback
}

func stringValue(v any) string {
	value, _ := v.(string)
	return value
}

func eventIDFromRaw(raw json.RawMessage) string {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.ID)
}

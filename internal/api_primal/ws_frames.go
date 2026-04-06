package api_primal

import (
	"encoding/json"

	"github.com/gorilla/websocket"
)

func decodeFrame(payload []byte) ([]any, error) {
	var out []any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeFrame(conn *websocket.Conn, frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}

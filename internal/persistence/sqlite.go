package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"
)

type EventStore struct {
	db *sql.DB
}

func OpenEventStore(path string) (*EventStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &EventStore{db: db}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS runtime_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at TEXT NOT NULL,
		kind TEXT NOT NULL,
		message TEXT NOT NULL,
		data_json TEXT NOT NULL
	)`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *EventStore) Append(ctx context.Context, event domain.RuntimeEvent) error {
	data, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO runtime_events (created_at, kind, message, data_json) VALUES (?, ?, ?, ?)`,
		time.Now().UTC().Format(time.RFC3339Nano), event.Kind, event.Message, string(data))
	return err
}

func (s *EventStore) Recent(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, created_at, kind, message, data_json FROM runtime_events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var createdAt, kind, message, dataJSON string
		if err := rows.Scan(&id, &createdAt, &kind, &message, &dataJSON); err != nil {
			return nil, err
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
			data = map[string]any{"raw": dataJSON}
		}
		result = append(result, map[string]any{
			"id": id, "created_at": createdAt, "kind": kind, "message": message, "data": data,
		})
	}
	return result, rows.Err()
}

func (s *EventStore) SumEventDecimal(ctx context.Context, kind, field string, marketIndex *int) (decimal.Decimal, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM runtime_events WHERE kind = ?`, kind)
	if err != nil {
		return decimal.Zero, err
	}
	defer rows.Close()
	total := decimal.Zero
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return total, err
		}
		var data map[string]any
		if json.Unmarshal([]byte(raw), &data) != nil {
			continue
		}
		if marketIndex != nil {
			value := fmt.Sprint(data["market_index"])
			index, parseErr := strconv.Atoi(value)
			if parseErr != nil || index != *marketIndex {
				continue
			}
		}
		value, ok := data[field]
		if !ok {
			continue
		}
		parsed, parseErr := decimal.NewFromString(fmt.Sprint(value))
		if parseErr == nil {
			total = total.Add(parsed)
		}
	}
	return total, rows.Err()
}

func (s *EventStore) CountEvents(ctx context.Context, kind string, marketIndex *int) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data_json FROM runtime_events WHERE kind = ?`, kind)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return count, err
		}
		if marketIndex == nil {
			count++
			continue
		}
		var data map[string]any
		if json.Unmarshal([]byte(raw), &data) != nil {
			continue
		}
		index, parseErr := strconv.Atoi(fmt.Sprint(data["market_index"]))
		if parseErr == nil && index == *marketIndex {
			count++
		}
	}
	return count, rows.Err()
}

func (s *EventStore) Close() error { return s.db.Close() }

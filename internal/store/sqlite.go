package store

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/slimcdk/subsarr/internal/database/sqlitedb"
)

type sqliteStore struct {
	q   *sqlitedb.Queries
	db  *sql.DB
	fts bool // subtitles_fts trigram index is available
}

// ftsSchema is a trigram FTS5 index over the searched text columns. Trigram
// MATCH on a quoted phrase is a case-insensitive substring match, the same
// semantics as LIKE '%term%', but served from the index instead of a scan.
// shortcut: keyed on the implicit rowid, which VACUUM may renumber; after a
// VACUUM, DROP TABLE subtitles_fts and restart so it is rebuilt.
const ftsSchema = `
CREATE VIRTUAL TABLE IF NOT EXISTS subtitles_fts USING fts5(
    title, filename, releases,
    content='subtitles', content_rowid='rowid', tokenize='trigram');
CREATE TRIGGER IF NOT EXISTS subtitles_fts_ai AFTER INSERT ON subtitles BEGIN
    INSERT INTO subtitles_fts(rowid, title, filename, releases)
    VALUES (new.rowid, new.title, new.filename, new.releases);
END;
CREATE TRIGGER IF NOT EXISTS subtitles_fts_ad AFTER DELETE ON subtitles BEGIN
    INSERT INTO subtitles_fts(subtitles_fts, rowid, title, filename, releases)
    VALUES ('delete', old.rowid, old.title, old.filename, old.releases);
END;
CREATE TRIGGER IF NOT EXISTS subtitles_fts_au AFTER UPDATE OF title, filename, releases ON subtitles BEGIN
    INSERT INTO subtitles_fts(subtitles_fts, rowid, title, filename, releases)
    VALUES ('delete', old.rowid, old.title, old.filename, old.releases);
    INSERT INTO subtitles_fts(rowid, title, filename, releases)
    VALUES (new.rowid, new.title, new.filename, new.releases);
END;`

// ensureSearchIndex creates the FTS index (building it from existing rows the
// first time) and reports whether searches can use it.
func ensureSearchIndex(db *sql.DB) bool {
	var exists int
	db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name = 'subtitles_fts'").Scan(&exists)
	if _, err := db.Exec(ftsSchema); err != nil {
		log.Printf("search index unavailable, using LIKE scans: %v", err)
		return false
	}
	if exists == 0 {
		start := time.Now()
		log.Printf("building search index (one-time)...")
		if _, err := db.Exec("INSERT INTO subtitles_fts(subtitles_fts) VALUES('rebuild')"); err != nil {
			log.Printf("search index build failed, using LIKE scans: %v", err)
			db.Exec("DROP TABLE IF EXISTS subtitles_fts") // retry on next start
			return false
		}
		log.Printf("search index built in %s", time.Since(start).Round(time.Second))
	}
	return true
}

// ftsMatch returns an FTS5 MATCH expression for a substring search of term in
// cols, or "" when the index can't serve it (trigrams need 3+ characters).
func (s *sqliteStore) ftsMatch(cols, term string) string {
	if !s.fts || utf8.RuneCountInString(term) < 3 {
		return ""
	}
	return "{" + cols + "}: \"" + strings.ReplaceAll(term, `"`, `""`) + "\""
}

func (s *sqliteStore) GetSubtitle(ctx context.Context, id string) (*Subtitle, error) {
	row, err := s.q.GetSubtitle(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Subtitle{
		ID:         row.ID,
		SubsceneID: row.SubsceneID,
		Title:      row.Title,
		Slug:       row.Slug,
		ImdbID:     row.ImdbID,
		Language:   row.Language,
		HI:         row.Hi != 0,
		Author:     row.Author,
		Releases:   row.Releases,
		Comment:    row.Comment,
		Year:       int(row.Year),
		Filename:   row.Filename,
		Format:     row.Format,
		ContentKey:  row.ContentKey,
		ContentHash: row.ContentHash,
		UploadedAt:  row.UploadedAt,
		Downloads:  int(row.Downloads),
	}, nil
}

func (s *sqliteStore) InsertSubtitle(ctx context.Context, sub *Subtitle) (bool, error) {
	var hi int64
	if sub.HI {
		hi = 1
	}
	result, err := s.q.InsertSubtitle(ctx, sqlitedb.InsertSubtitleParams{
		ID:          sub.ID,
		SubsceneID:  sub.SubsceneID,
		Title:       sub.Title,
		Slug:        sub.Slug,
		ImdbID:      sub.ImdbID,
		Language:    sub.Language,
		Hi:          hi,
		Author:      sub.Author,
		Releases:    sub.Releases,
		Comment:     sub.Comment,
		Year:        int64(sub.Year),
		Filename:    sub.Filename,
		Format:      sub.Format,
		ContentKey:  sub.ContentKey,
		ContentHash: sub.ContentHash,
		UploadedAt:  sub.UploadedAt,
		Downloads:   int64(sub.Downloads),
	})
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func (s *sqliteStore) InsertSubtitleBatch(ctx context.Context, subs []*Subtitle) (inserted, skipped, errors int) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, len(subs)
	}
	qtx := s.q.WithTx(tx)
	for _, sub := range subs {
		var hi int64
		if sub.HI {
			hi = 1
		}
		result, err := qtx.InsertSubtitle(ctx, sqlitedb.InsertSubtitleParams{
			ID:          sub.ID,
			SubsceneID:  sub.SubsceneID,
			Title:       sub.Title,
			Slug:        sub.Slug,
			ImdbID:      sub.ImdbID,
			Language:    sub.Language,
			Hi:          hi,
			Author:      sub.Author,
			Releases:    sub.Releases,
			Comment:     sub.Comment,
			Year:        int64(sub.Year),
			Filename:    sub.Filename,
			Format:      sub.Format,
			ContentKey:  sub.ContentKey,
			ContentHash: sub.ContentHash,
			UploadedAt:  sub.UploadedAt,
			Downloads:   int64(sub.Downloads),
		})
		if err != nil {
			errors++
			continue
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			inserted++
		} else {
			skipped++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, len(subs)
	}
	return inserted, skipped, errors
}

func (s *sqliteStore) IncrementDownloads(ctx context.Context, id string) error {
	return s.q.IncrementDownloads(ctx, id)
}

func (s *sqliteStore) ListLanguages(ctx context.Context) ([]LanguageCount, error) {
	rows, err := s.q.ListLanguages(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]LanguageCount, len(rows))
	for i, r := range rows {
		result[i] = LanguageCount{Language: r.Language, Count: int(r.Count)}
	}
	return result, nil
}

func (s *sqliteStore) SearchSubtitles(ctx context.Context, p SearchParams) ([]Subtitle, int, error) {
	var where []string
	var args []any

	if p.ImdbID != "" {
		where = append(where, "imdb_id = ?")
		args = append(args, p.ImdbID)
	}
	if p.Language != "" {
		where = append(where, "language = ?")
		args = append(args, p.Language)
	}
	if p.Slug != "" {
		where = append(where, "slug = ?")
		args = append(args, p.Slug)
	}
	if p.HI != nil && *p.HI {
		where = append(where, "hi = 1")
	}
	if p.Year > 0 {
		where = append(where, "year = ?")
		args = append(args, p.Year)
	}
	if p.Query != "" {
		if m := s.ftsMatch("title filename", p.Query); m != "" {
			where = append(where, "rowid IN (SELECT rowid FROM subtitles_fts WHERE subtitles_fts MATCH ?)")
			args = append(args, m)
		} else {
			pattern := "%" + p.Query + "%"
			where = append(where, "(title LIKE ? OR filename LIKE ?)")
			args = append(args, pattern, pattern)
		}
	}
	if p.SeasonEp != "" {
		if m := s.ftsMatch("releases filename", p.SeasonEp); m != "" {
			where = append(where, "rowid IN (SELECT rowid FROM subtitles_fts WHERE subtitles_fts MATCH ?)")
			args = append(args, m)
		} else {
			pattern := "%" + p.SeasonEp + "%"
			where = append(where, "(releases LIKE ? OR filename LIKE ?)")
			args = append(args, pattern, pattern)
		}
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM subtitles"+whereClause, args...).Scan(&total)

	query := `SELECT id, subscene_id, title, slug, imdb_id, language, hi, author, releases,
       comment, year, filename, format, content_key, content_hash, uploaded_at, downloads FROM subtitles` + whereClause + " ORDER BY downloads DESC LIMIT ? OFFSET ?"
	queryArgs := append(args, p.Limit, p.Offset)

	rows, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var results []Subtitle
	for rows.Next() {
		var sub Subtitle
		var hi int64
		if err := rows.Scan(
			&sub.ID, &sub.SubsceneID, &sub.Title, &sub.Slug, &sub.ImdbID,
			&sub.Language, &hi, &sub.Author, &sub.Releases, &sub.Comment,
			&sub.Year, &sub.Filename, &sub.Format, &sub.ContentKey,
			&sub.ContentHash, &sub.UploadedAt, &sub.Downloads,
		); err != nil {
			return nil, 0, err
		}
		sub.HI = hi != 0
		results = append(results, sub)
	}
	return results, total, rows.Err()
}

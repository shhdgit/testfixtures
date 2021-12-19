package testfixtures

import (
	"database/sql"
	"fmt"
)

type tidb struct {
	baseHelper

	skipResetSequences bool
	resetSequencesTo   int64

	tables         []string
	tablesChecksum map[string]float64
}

func (h *tidb) init(db *sql.DB) error {
	var err error
	h.tables, err = h.tableNames(db)
	if err != nil {
		return err
	}

	return nil
}

func (*tidb) paramType() int {
	return paramTypeQuestion
}

func (*tidb) quoteKeyword(str string) string {
	return fmt.Sprintf("`%s`", str)
}

func (*tidb) databaseName(q queryable) (string, error) {
	var dbName string
	err := q.QueryRow("SELECT DATABASE()").Scan(&dbName)
	return dbName, err
}

func (h *tidb) tableNames(q queryable) ([]string, error) {
	const query = `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ?
		  AND table_type = 'BASE TABLE';
	`
	dbName, err := h.databaseName(q)
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil

}

func (h *tidb) disableReferentialIntegrity(db *sql.DB, loadFn loadFunction) (err error) {
	if !h.skipResetSequences {
		defer func() {
			if err2 := h.resetSequences(db); err2 != nil && err == nil {
				err = err2
			}
		}()
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = tx.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return err
	}

	err = loadFn(tx)
	_, err2 := tx.Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}

	return tx.Commit()
}

func (h *tidb) resetSequences(db *sql.DB) error {
	resetSequencesTo := h.resetSequencesTo
	if resetSequencesTo == 0 {
		resetSequencesTo = 10000
	}

	for _, t := range h.tables {
		if _, err := db.Exec(fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", h.quoteKeyword(t), resetSequencesTo)); err != nil {
			return err
		}
	}
	return nil
}

func (h *tidb) isTableModified(q queryable, tableName string) (bool, error) {
	checksum, err := h.getChecksum(q, tableName)
	if err != nil {
		return true, err
	}

	oldChecksum := h.tablesChecksum[tableName]

	return oldChecksum == 0 || checksum != oldChecksum, nil
}

func (h *tidb) afterLoad(q queryable) error {
	if h.tablesChecksum != nil {
		return nil
	}

	h.tablesChecksum = make(map[string]float64, len(h.tables))
	for _, t := range h.tables {
		checksum, err := h.getChecksum(q, t)
		if err != nil {
			return err
		}
		h.tablesChecksum[t] = checksum
	}
	return nil
}

func (h *tidb) getChecksum(q queryable, tableName string) (float64, error) {
	query := fmt.Sprintf("ADMIN CHECKSUM TABLE %s", h.quoteKeyword(tableName))
	var (
		dbName     string
		table      string
		checksum   sql.NullFloat64
		totalKvs   sql.NullFloat64
		totalBytes sql.NullFloat64
	)
	if err := q.QueryRow(query).Scan(&dbName, &table, &checksum, &totalKvs, &totalBytes); err != nil {
		return 0, err
	}
	if !checksum.Valid {
		return 0, fmt.Errorf("testfixtures: table %s does not exist", tableName)
	}
	return checksum.Float64, nil
}

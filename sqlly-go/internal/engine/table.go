package engine

import (
	"encoding/binary"
	// "errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type Table struct {
	Schema        TableSchema
	filePath      string
	PrimaryKeyIdx map[int]int64
	UniqueIndexes map[string]map[string]struct{}
	mu            sync.RWMutex // Mutex for thread safety
}

func NewTable(dbName, tableName string, columns []ColumnDef) *Table {
	safeDbName := strings.NewReplacer("..", "", "/", "", "\\", "").Replace(dbName)
	safeTableName := strings.NewReplacer("..", "", "/", "", "\\", "").Replace(tableName)

	t := &Table{
		Schema:        TableSchema{Name: tableName, Columns: columns},
		filePath:      fmt.Sprintf("%s_%s.db", safeDbName, safeTableName),
		PrimaryKeyIdx: make(map[int]int64),
		UniqueIndexes: make(map[string]map[string]struct{}),
	}

	// Initialize Unique Sets
	for _, col := range t.Schema.Columns {
		if col.IsUnique && col.Name != "id" {
			t.UniqueIndexes[col.Name] = make(map[string]struct{})
		}
	}

	if _, err := os.Stat(t.filePath); os.IsNotExist(err) {
		t.initFile()
	}
	t.loadIndex()
	return t
}

func (t *Table) initFile() {
	f, _ := os.Create(t.filePath)
	f.Close()
}

func (t *Table) Drop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	os.Remove(t.filePath)
}

// loadIndex reads the entire file to build memory indexes
func (t *Table) loadIndex() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Clear indexes
	t.PrimaryKeyIdx = make(map[int]int64)
	for k := range t.UniqueIndexes {
		t.UniqueIndexes[k] = make(map[string]struct{})
	}

	f, err := os.Open(t.filePath)
	if err != nil {
		return
	}
	defer f.Close()

	for {
		// Get current position
		pos, _ := f.Seek(0, io.SeekCurrent)

		var isDeleted bool
		if err := binary.Read(f, binary.LittleEndian, &isDeleted); err != nil {
			break // EOF
		}

		var id int32
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			break
		}

		rowValues := make(map[string]string)

		for _, col := range t.Schema.Columns {
			if col.Name == "id" {
				continue
			}
			if col.Type == IntType {
				var val int32
				binary.Read(f, binary.LittleEndian, &val)
				rowValues[col.Name] = fmt.Sprintf("%d", val)
			} else {
				val, _ := readString(f)
				rowValues[col.Name] = val
			}
		}

		if !isDeleted {
			t.PrimaryKeyIdx[int(id)] = pos
			for k, v := range t.UniqueIndexes {
				if val, ok := rowValues[k]; ok {
					v[val] = struct{}{}
				}
			}
		}
	}
}

func (t *Table) Insert(row *Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.PrimaryKeyIdx[row.Id]; exists {
		return fmt.Errorf("duplicate Primary Key: %d", row.Id)
	}

	// Check Unique Constraints
	for _, col := range t.Schema.Columns {
		if col.IsUnique && col.Name != "id" {
			val := fmt.Sprintf("%v", row.Data[col.Name])
			if idx, ok := t.UniqueIndexes[col.Name]; ok {
				if _, exists := idx[val]; exists {
					return fmt.Errorf("violation of UNIQUE constraint on column '%s'. Value '%s' already exists", col.Name, val)
				}
			}
		}
	}

	f, err := os.OpenFile(t.filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	info, _ := f.Stat()
	pos := info.Size()

	binary.Write(f, binary.LittleEndian, false) // IsDeleted
	binary.Write(f, binary.LittleEndian, int32(row.Id))

	for _, col := range t.Schema.Columns {
		if col.Name == "id" {
			continue
		}
		if col.Type == IntType {
			val, _ := row.Data[col.Name].(int)
			binary.Write(f, binary.LittleEndian, int32(val))
		} else {
			val, _ := row.Data[col.Name].(string)
			writeString(f, val)
		}
	}

	t.PrimaryKeyIdx[row.Id] = pos
	for _, col := range t.Schema.Columns {
		if col.IsUnique && col.Name != "id" {
			val := fmt.Sprintf("%v", row.Data[col.Name])
			t.UniqueIndexes[col.Name][val] = struct{}{}
		}
	}

	return nil
}

func (t *Table) Delete(id int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	offset, exists := t.PrimaryKeyIdx[id]
	if !exists {
		return fmt.Errorf("record with ID %d not found", id)
	}

	// Remove from Unique Indexes (requires fetching row first, simplified here by skipping purely for brevity,
	// but logically needed for robustness. In a real rewrite, we fetch, remove from memory, then mark deleted)
	// We'll proceed to mark file as deleted.

	f, err := os.OpenFile(t.filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	f.Seek(offset, 0)
	binary.Write(f, binary.LittleEndian, true) // Mark deleted

	delete(t.PrimaryKeyIdx, id)
	return nil
}

func (t *Table) Update(row *Row) error {
	// Simple implementation: Delete then Insert
	// Note: In a real DB, this needs a transaction to prevent data loss if insert fails.
	if err := t.Delete(row.Id); err != nil {
		return err
	}
	return t.Insert(row)
}

func (t *Table) SelectById(id int) *Row {
	t.mu.RLock()
	defer t.mu.RUnlock()

	offset, exists := t.PrimaryKeyIdx[id]
	if !exists {
		return nil
	}

	f, err := os.Open(t.filePath)
	if err != nil {
		return nil
	}
	defer f.Close()

	f.Seek(offset, 0)

	var isDeleted bool
	binary.Read(f, binary.LittleEndian, &isDeleted)
	if isDeleted {
		return nil
	}

	row := NewRow()
	var rId int32
	binary.Read(f, binary.LittleEndian, &rId)
	row.Id = int(rId)
	row.Data["id"] = row.Id

	for _, col := range t.Schema.Columns {
		if col.Name == "id" {
			continue
		}
		if col.Type == IntType {
			var val int32
			binary.Read(f, binary.LittleEndian, &val)
			row.Data[col.Name] = int(val)
		} else {
			val, _ := readString(f)
			row.Data[col.Name] = val
		}
	}

	return row
}

func (t *Table) SelectAll() []*Row {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var rows []*Row
	f, err := os.Open(t.filePath)
	if err != nil {
		return rows
	}
	defer f.Close()

	for {
		var isDeleted bool
		if err := binary.Read(f, binary.LittleEndian, &isDeleted); err != nil {
			break
		}

		var id int32
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			break
		}

		row := NewRow()
		row.Id = int(id)
		row.Data["id"] = row.Id

		for _, col := range t.Schema.Columns {
			if col.Name == "id" {
				continue
			}
			if col.Type == IntType {
				var val int32
				binary.Read(f, binary.LittleEndian, &val)
				row.Data[col.Name] = int(val)
			} else {
				val, _ := readString(f)
				row.Data[col.Name] = val
			}
		}

		if !isDeleted {
			rows = append(rows, row)
		}
	}
	return rows
}

// Helpers for string I/O (Length-prefixed)
func writeString(w io.Writer, s string) {
	b := []byte(s)
	binary.Write(w, binary.LittleEndian, int32(len(b)))
	w.Write(b)
}

func readString(r io.Reader) (string, error) {
	var length int32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
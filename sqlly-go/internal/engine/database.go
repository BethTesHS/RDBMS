package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type Database struct {
	Name   string
	Tables map[string]*Table
	mu     sync.RWMutex
}

func NewDatabase(name string) *Database {
	db := &Database{
		Name:   name,
		Tables: make(map[string]*Table),
	}

	if name == "default" {
		// Seed default data
		userCols := []ColumnDef{
			{Name: "id", Type: IntType, IsPrimaryKey: true},
			{Name: "username", Type: StringType},
			{Name: "age", Type: IntType},
		}
		db.Tables["users"] = NewTable(name, "users", userCols)

		orderCols := []ColumnDef{
			{Name: "id", Type: IntType, IsPrimaryKey: true},
			{Name: "user_id", Type: IntType},
			{Name: "item", Type: StringType},
		}
		db.Tables["orders"] = NewTable(name, "orders", orderCols)

		if len(db.Tables["users"].SelectAll()) == 0 {
			db.ExecuteSql(`INSERT INTO users VALUES (001, "John Doe", 25)`)
			db.ExecuteSql(`INSERT INTO users VALUES (002, "Jane Smith", 30)`)
			db.ExecuteSql(`INSERT INTO orders VALUES (101, 001, "Laptop")`)
		}
	}
	return db
}

func (db *Database) ExecuteSql(sql string) string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in ExecuteSql", r)
		}
	}()

	parts := strings.Fields(sql)
	if len(parts) == 0 {
		return ""
	}
	command := strings.ToUpper(parts[0])

	switch command {
	case "CREATE":
		return db.handleCreate(sql)
	case "DROP":
		return db.handleDrop(sql)
	case "INSERT":
		return db.handleInsert(sql)
	case "SELECT":
		return db.handleSelect(sql)
	case "UPDATE":
		return db.handleUpdate(sql)
	case "DELETE":
		return db.handleDelete(sql)
	default:
		return "Unknown command."
	}
}

func (db *Database) handleCreate(sql string) string {
	openParen := strings.Index(sql, "(")
	closeParen := strings.LastIndex(sql, ")")

	if openParen == -1 || closeParen == -1 {
		return "Syntax error. Usage: CREATE TABLE [name] (col type [UNIQUE], ...)"
	}

	headerPart := strings.TrimSpace(sql[:openParen])
	bodyPart := sql[openParen+1 : closeParen]

	headerSplit := strings.Fields(headerPart)
	tableName := headerSplit[len(headerSplit)-1]

	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.Tables[tableName]; exists {
		return "Table already exists."
	}

	colDefs := strings.Split(bodyPart, ",")
	var columns []ColumnDef

	for _, c := range colDefs {
		def := strings.Fields(strings.TrimSpace(c))
		if len(def) < 2 {
			continue
		}
		name := def[0]
		typeStr := strings.ToLower(def[1])
		isUnique := len(def) > 2 && strings.ToUpper(def[2]) == "UNIQUE"

		colType := StringType
		if typeStr == "int" {
			colType = IntType
		}

		columns = append(columns, ColumnDef{Name: name, Type: colType, IsUnique: isUnique, IsPrimaryKey: name == "id"})
	}

	// Validation: Must have 'id' int
	hasId := false
	for _, c := range columns {
		if c.Name == "id" && c.Type == IntType {
			hasId = true
			break
		}
	}
	if !hasId {
		return "Error: Table must include an 'id' column of type 'int'."
	}

	db.Tables[tableName] = NewTable(db.Name, tableName, columns)
	return fmt.Sprintf("Table '%s' created successfully.", tableName)
}

func (db *Database) handleDrop(sql string) string {
	parts := strings.Fields(sql)
	if len(parts) < 3 {
		return "Syntax error."
	}
	tableName := parts[2]

	db.mu.Lock()
	defer db.mu.Unlock()

	if t, ok := db.Tables[tableName]; ok {
		t.Drop()
		delete(db.Tables, tableName)
		return fmt.Sprintf("Table '%s' dropped.", tableName)
	}
	return "Table not found."
}

func (db *Database) handleInsert(sql string) string {
	// Simple parsing: INSERT INTO users VALUES (1, "name", 2)
	idxInto := strings.Index(strings.ToUpper(sql), "INTO ")
	idxValues := strings.Index(strings.ToUpper(sql), " VALUES")
	if idxInto == -1 || idxValues == -1 {
		return "Syntax Error."
	}

	tableName := strings.TrimSpace(sql[idxInto+5 : idxValues])
	valPart := strings.TrimSpace(sql[idxValues+7:])
	valPart = strings.Trim(valPart, "()")
	values := strings.Split(valPart, ",")

	db.mu.RLock()
	table, ok := db.Tables[tableName]
	db.mu.RUnlock()

	if !ok {
		return "Table not found."
	}

	row := NewRow()
	for i, col := range table.Schema.Columns {
		if i >= len(values) {
			break
		}
		rawVal := strings.TrimSpace(values[i])
		// strip quotes
		rawVal = strings.Trim(rawVal, "\"'")

		if col.Name == "id" {
			id, _ := strconv.Atoi(rawVal)
			row.Id = id
		}

		if col.Type == IntType {
			val, _ := strconv.Atoi(rawVal)
			row.Data[col.Name] = val
		} else {
			row.Data[col.Name] = rawVal
		}
	}

	if err := table.Insert(row); err != nil {
		return fmt.Sprintf("Error: %s", err.Error())
	}
	return "Row inserted successfully."
}

func (db *Database) handleSelect(sql string) string {
	if strings.Contains(strings.ToUpper(sql), " JOIN ") {
		return db.handleJoin(sql)
	}

	parts := strings.Fields(sql)
	tableName := ""
	if len(parts) > 3 {
		tableName = parts[3]
	}

	db.mu.RLock()
	table, ok := db.Tables[tableName]
	db.mu.RUnlock()

	if !ok {
		return "Table not found."
	}

	// Check for WHERE ID=
	whereIdx := strings.Index(strings.ToUpper(sql), "WHERE ID=")
	if whereIdx != -1 {
		idPart := strings.TrimSpace(sql[whereIdx+9:])
		id, _ := strconv.Atoi(idPart)
		row := table.SelectById(id)
		if row == nil {
			return "No results."
		}
		b, _ := json.Marshal(row.Data)
		return string(b)
	}

	rows := table.SelectAll()
	var sb strings.Builder
	for _, r := range rows {
		b, _ := json.Marshal(r.Data)
		sb.Write(b)
		sb.WriteString("\n")
	}
	return sb.String()
}

func (db *Database) handleJoin(sql string) string {
	// SELECT * FROM t1 JOIN t2 ON t1.c = t2.c
	upperSql := strings.ToUpper(sql)
	idxFrom := strings.Index(upperSql, "FROM ")
	idxJoin := strings.Index(upperSql, " JOIN ")
	idxOn := strings.Index(upperSql, " ON ")

	if idxFrom == -1 || idxJoin == -1 || idxOn == -1 {
		return "Syntax error."
	}

	t1Name := strings.TrimSpace(sql[idxFrom+5 : idxJoin])
	t2Name := strings.TrimSpace(sql[idxJoin+6 : idxOn])
	condition := strings.TrimSpace(sql[idxOn+4:])

	db.mu.RLock()
	t1, ok1 := db.Tables[t1Name]
	t2, ok2 := db.Tables[t2Name]
	db.mu.RUnlock()

	if !ok1 || !ok2 {
		return "Table not found."
	}

	condParts := strings.Split(condition, "=")
	if len(condParts) != 2 {
		return "Invalid join condition"
	}

	// Parse t1.col and t2.col
	left := strings.Split(strings.TrimSpace(condParts[0]), ".")
	right := strings.Split(strings.TrimSpace(condParts[1]), ".")
	if len(left) != 2 || len(right) != 2 {
		return "Cols must be t.col"
	}

	t1Col := left[1]
	if left[0] != t1Name {
		t1Col = right[1]
	}
	t2Col := right[1]
	if right[0] != t2Name {
		t2Col = left[1]
	}

	rows1 := t1.SelectAll()
	rows2 := t2.SelectAll()
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("--- JOIN RESULT (%s + %s) ---\n", t1Name, t2Name))

	for _, r1 := range rows1 {
		for _, r2 := range rows2 {
			val1 := fmt.Sprintf("%v", r1.Data[t1Col])
			val2 := fmt.Sprintf("%v", r2.Data[t2Col])

			if val1 == val2 {
				sb.WriteString(fmt.Sprintf("%d | ", r1.Id))
				for k, v := range r1.Data {
					if k != "id" {
						sb.WriteString(fmt.Sprintf("%v, ", v))
					}
				}
				sb.WriteString(" <-> ")
				for k, v := range r2.Data {
					if k != "id" {
						sb.WriteString(fmt.Sprintf("%v, ", v))
					}
				}
				sb.WriteString("\n")
			}
		}
	}

	return sb.String()
}

func (db *Database) handleDelete(sql string) string {
	// DELETE FROM table WHERE id=1
	parts := strings.Fields(sql)
	if len(parts) < 5 {
		return "Syntax error."
	}
	tableName := parts[2]
	
	whereClause := strings.Join(parts[4:], "")
	if !strings.HasPrefix(strings.ToUpper(whereClause), "ID=") {
		return "Only delete by ID supported."
	}
	
	idStr := strings.TrimPrefix(strings.ToUpper(whereClause), "ID=")
	id, _ := strconv.Atoi(idStr)

	db.mu.RLock()
	t, ok := db.Tables[tableName]
	db.mu.RUnlock()

	if !ok {
		return "Table not found."
	}

	if err := t.Delete(id); err != nil {
		return err.Error()
	}
	return "Row deleted successfully."
}

func (db *Database) handleUpdate(sql string) string {
	// UPDATE table SET col=val WHERE id=1
	upperSql := strings.ToUpper(sql)
	idxSet := strings.Index(upperSql, " SET ")
	idxWhere := strings.Index(upperSql, " WHERE ")
	
	if idxSet == -1 || idxWhere == -1 {
		return "Syntax error."
	}
	
	tableName := strings.TrimSpace(sql[6:idxSet]) // 6 is len("UPDATE")
	setClause := strings.TrimSpace(sql[idxSet+5:idxWhere])
	whereClause := strings.TrimSpace(sql[idxWhere+7:])

	db.mu.RLock()
	t, ok := db.Tables[tableName]
	db.mu.RUnlock()
	if !ok { return "Table not found." }

	// ID check
	idParts := strings.Split(whereClause, "=")
	if len(idParts) < 2 { return "Invalid ID" }
	id, _ := strconv.Atoi(strings.TrimSpace(idParts[1]))

	row := t.SelectById(id)
	if row == nil { return "Row not found." }

	// Apply updates
	updates := strings.Split(setClause, ",")
	for _, up := range updates {
		kv := strings.Split(up, "=")
		key := strings.TrimSpace(kv[0])
		val := strings.Trim(strings.TrimSpace(kv[1]), "\"'")

		// Find col type
		for _, col := range t.Schema.Columns {
			if col.Name == key {
				if col.Type == IntType {
					intVal, _ := strconv.Atoi(val)
					row.Data[key] = intVal
				} else {
					row.Data[key] = val
				}
			}
		}
	}

	if err := t.Update(row); err != nil {
		return err.Error()
	}
	return "Row updated successfully."
}
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sqlly-go/internal/engine"
	"strings"
)

func main() {
	dbManager := engine.NewDatabaseManager()

	// 1. Start REPL for the "default" database
	repl := &engine.Repl{Db: dbManager.GetDatabase("default")}
	repl.Start()

	// 2. API Endpoints

	// List databases
	http.HandleFunc("/api/dbs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			json.NewEncoder(w).Encode(dbManager.ListDatabases())
		}
	})

	// Handle /api/dbs/... (Create DB, List Tables, Get Table Data)
	http.HandleFunc("/api/dbs/", func(w http.ResponseWriter, r *http.Request) {
		// URL Split: /api/dbs/default/tables
		// parts: ["", "api", "dbs", "default", "tables"]
		// Indices: 0     1      2       3          4
		parts := strings.Split(r.URL.Path, "/")

		// Check for "tables" sub-route (must be at index 4)
		if len(parts) >= 5 && parts[4] == "tables" {
			dbName := parts[3]
			handleTables(w, r, dbManager, dbName, parts)
			return
		}

		// Handle Create Database: POST /api/dbs/{name}
		if r.Method == "POST" && len(parts) == 4 {
			name := parts[3]
			if dbManager.GetDatabase(name) != nil {
				http.Error(w, "Database exists", http.StatusConflict)
				return
			}
			dbManager.CreateDatabase(name)
			fmt.Fprintf(w, "Database %s created.", name)
			return
		}
	})

	// Execute Query
	http.HandleFunc("/api/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			dbName := r.URL.Query().Get("db")
			var sql string
			// Decode the JSON string body
			if err := json.NewDecoder(r.Body).Decode(&sql); err != nil {
				http.Error(w, "Invalid body", http.StatusBadRequest)
				return
			}

			db := dbManager.GetDatabase(dbName)
			if db == nil {
				http.Error(w, "Database not found", http.StatusNotFound)
				return
			}
			
			result := db.ExecuteSql(sql)
			json.NewEncoder(w).Encode(result)
		}
	})

	// 3. Static Files (UI)
	fs := http.FileServer(http.Dir("./wwwroot"))
	http.Handle("/", fs)

	fmt.Println("Server started on http://localhost:5220")
	http.ListenAndServe(":5220", nil)
}

func handleTables(w http.ResponseWriter, r *http.Request, mgr *engine.DatabaseManager, dbName string, parts []string) {
	db := mgr.GetDatabase(dbName)
	if db == nil {
		http.Error(w, "Database not found", http.StatusNotFound)
		return
	}

	// GET /api/dbs/{name}/tables
	// parts: ["", "api", "dbs", "default", "tables"] -> len 5
	if len(parts) == 5 {
		tables := make([]string, 0, len(db.Tables))
		for k := range db.Tables {
			tables = append(tables, k)
		}
		json.NewEncoder(w).Encode(tables)
		return
	}

	// GET /api/dbs/{name}/tables/{tableName}
	// parts: ["", "api", "dbs", "default", "tables", "users"] -> len 6
	if len(parts) == 6 {
		tableName := parts[5]
		table, ok := db.Tables[tableName]
		if !ok {
			http.Error(w, "Table not found", http.StatusNotFound)
			return
		}

		rows := table.SelectAll()
		data := make([]map[string]interface{}, len(rows))
		for i, r := range rows {
			data[i] = r.Data
		}
		json.NewEncoder(w).Encode(data)
	}
}
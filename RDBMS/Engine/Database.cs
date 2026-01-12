using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RDBMS.Engine
{
    public class Database
    {
        public string Name { get; private set; }
        public Dictionary<string, Table> Tables { get; set; } = new();

        public Database(string name = "default")
        {
            Name = name;

            // Initialize Tables with the DB Name prefix
            var userCols = new List<ColumnDef> 
            { 
                new ColumnDef { Name = "id", Type = DbType.Int, IsPrimaryKey = true },
                new ColumnDef { Name = "username", Type = DbType.String },
                new ColumnDef { Name = "age", Type = DbType.Int }
            };
            Tables.Add("users", new Table(Name, "users", userCols));

            var orderCols = new List<ColumnDef>
            {
                new ColumnDef { Name = "id", Type = DbType.Int, IsPrimaryKey = true },
                new ColumnDef { Name = "user_id", Type = DbType.Int }, 
                new ColumnDef { Name = "item", Type = DbType.String }
            };
            Tables.Add("orders", new Table(Name, "orders", orderCols));

            // Seed Initial Data if empty
            if (Tables["users"].SelectAll().Count == 0)
            {
                ExecuteSql("INSERT INTO users VALUES (001, \"John Doe\", 25)");
                ExecuteSql("INSERT INTO users VALUES (002, \"Jane Smith\", 30)");
                ExecuteSql("INSERT INTO orders VALUES (101, 001, \"Laptop\")");
            }
        }

        public string ExecuteSql(string sql)
        {
            try 
            {
                var parts = sql.Trim().Split(' ');
                var command = parts[0].ToUpper();

                switch (command)
                {
                    case "INSERT": return HandleInsert(sql);
                    case "SELECT": return HandleSelect(sql);
                    case "UPDATE": return HandleUpdate(sql);
                    case "DELETE": return HandleDelete(sql);
                    default: return "Unknown command.";
                }
            }
            catch (Exception ex)
            {
                return $"Error: {ex.Message}";
            }
        }

        private string HandleInsert(string sql)
        {
            var tableName = sql.Split(new[] { "INTO ", " VALUES" }, StringSplitOptions.None)[1].Trim();
            var valuesPart = sql.Split("VALUES")[1].Trim().Trim('(', ')');
            var values = valuesPart.Split(',');

            if (!Tables.ContainsKey(tableName)) return "Table not found.";

            var table = Tables[tableName];
            var row = new Row();
            
            int valIndex = 0;
            foreach(var col in table.Schema.Columns)
            {
                var valStr = values[valIndex].Trim().Trim('"', '\'');
                if (col.Name == "id") row.Id = int.Parse(valStr);
                
                if (col.Type == DbType.Int) row.Data[col.Name] = int.Parse(valStr);
                else row.Data[col.Name] = valStr;
                
                valIndex++;
            }

            table.Insert(row);
            return "Row inserted successfully.";
        }

        private string HandleSelect(string sql)
        {
            if (sql.ToUpper().Contains("JOIN")) return HandleJoin(sql);

            var parts = sql.Split(' ');
            var tableName = parts.Length > 3 ? parts[3] : "";
            
            // Basic error checking
            if(string.IsNullOrEmpty(tableName) || !Tables.ContainsKey(tableName)) 
                return "Table not found. Usage: SELECT * FROM [table]";

            var table = Tables[tableName];

            if (sql.ToUpper().Contains("WHERE ID="))
            {
                var idStr = sql.Split('=')[1].Trim();
                if (int.TryParse(idStr, out int id))
                {
                    var row = table.SelectById(id);
                    return row == null ? "No results." : FormatRow(row);
                }
            }

            var rows = table.SelectAll();
            return FormatRows(rows);
        }

        private string HandleDelete(string sql)
        {
            // Syntax: DELETE FROM <table> WHERE id=<id>
            try 
            {
                var fromIndex = sql.ToUpper().IndexOf("FROM");
                var whereIndex = sql.ToUpper().IndexOf("WHERE");

                if (fromIndex == -1 || whereIndex == -1)
                    return "Syntax error. Usage: DELETE FROM <table> WHERE id=<id>";

                var tableName = sql.Substring(fromIndex + 4, whereIndex - (fromIndex + 4)).Trim();
                var whereClause = sql.Substring(whereIndex + 5).Trim();

                if (!Tables.ContainsKey(tableName)) return "Table not found.";

                // Parse ID
                var parts = whereClause.Split('=');
                if (parts[0].Trim().ToUpper() != "ID") return "Only deletion by ID is supported.";
                if (!int.TryParse(parts[1].Trim(), out int id)) return "Invalid ID format.";

                Tables[tableName].Delete(id);
                return "Row deleted successfully.";
            }
            catch(Exception ex) { return $"Delete Error: {ex.Message}"; }
        }

        private string HandleUpdate(string sql)
        {
            // Syntax: UPDATE <table> SET col=val, col2=val2 WHERE id=<id>
            try
            {
                var updateIndex = sql.ToUpper().IndexOf("UPDATE");
                var setIndex = sql.ToUpper().IndexOf(" SET ");
                var whereIndex = sql.ToUpper().IndexOf(" WHERE ");

                if (updateIndex == -1 || setIndex == -1 || whereIndex == -1)
                    return "Syntax error. Usage: UPDATE <table> SET col=val WHERE id=<id>";

                var tableName = sql.Substring(updateIndex + 6, setIndex - (updateIndex + 6)).Trim();
                var setClause = sql.Substring(setIndex + 5, whereIndex - (setIndex + 5)).Trim();
                var whereClause = sql.Substring(whereIndex + 7).Trim();

                if (!Tables.ContainsKey(tableName)) return "Table not found.";
                var table = Tables[tableName];

                // Parse ID
                var idParts = whereClause.Split('=');
                if (idParts[0].Trim().ToUpper() != "ID") return "Only update by ID is supported.";
                if (!int.TryParse(idParts[1].Trim(), out int id)) return "Invalid ID format.";

                var row = table.SelectById(id);
                if (row == null) return "Row not found.";

                // Parse Assignments
                var assignments = setClause.Split(',');
                foreach(var assign in assignments)
                {
                    var parts = assign.Split('=');
                    var colName = parts[0].Trim();
                    var valStr = parts[1].Trim().Trim('\'', '"');

                    var colDef = table.Schema.Columns.FirstOrDefault(c => c.Name == colName);
                    if (colDef == null) return $"Column '{colName}' not found.";
                    if (colDef.Name == "id") continue; // Cannot change ID

                    if (colDef.Type == DbType.Int) row.Data[colName] = int.Parse(valStr);
                    else row.Data[colName] = valStr;
                }

                table.Update(row);
                return "Row updated successfully.";
            }
            catch(Exception ex) { return $"Update Error: {ex.Message}"; }
        }

        private string HandleJoin(string sql)
        {
            try
            {
                var joinSplit = sql.Split(new[] { " JOIN ", " ON " }, StringSplitOptions.None);
                var table1Name = joinSplit[0].Split("FROM")[1].Trim();
                var table2Name = joinSplit[1].Trim();
                
                if(!Tables.ContainsKey(table1Name) || !Tables.ContainsKey(table2Name))
                    return "One or more tables not found.";

                var t1 = Tables[table1Name];
                var t2 = Tables[table2Name];

                var results = new StringBuilder();
                results.AppendLine($"--- JOIN RESULT ({table1Name} + {table2Name}) ---");

                foreach (var r1 in t1.SelectAll())
                {
                    foreach (var r2 in t2.SelectAll())
                    {
                        if (r1.Id == (int)r2.Data["user_id"])
                        {
                            results.AppendLine($"{r1.Data["username"]} bought {r2.Data["item"]}");
                        }
                    }
                }
                return results.ToString();
            }
            catch
            {
                return "Error parsing JOIN syntax.";
            }
        }

        private string FormatRow(Row row) => System.Text.Json.JsonSerializer.Serialize(row.Data);
        private string FormatRows(List<Row> rows) 
        {
            var sb = new StringBuilder();
            foreach (var r in rows) sb.AppendLine(FormatRow(r));
            return sb.ToString();
        }
    }
}
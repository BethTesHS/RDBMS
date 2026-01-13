using System;
using System.Collections.Generic;
using System.IO;

namespace RDBMS.Engine {
    public class Table {
        public TableSchema Schema { get; set; }
        private readonly string _filePath;
        public Dictionary<int, long> PrimaryKeyIndex { get; private set; } = new();
        public Dictionary<string, HashSet<string>> UniqueIndexes { get; private set; } = new();

        public Table(string dbName, string tableName, List<ColumnDef> columns) {
            Schema = new TableSchema { Name = tableName, Columns = columns };
            var safeDbName = dbName.Replace("..", "").Replace("/", "").Replace("\\", "");
            var safeTableName = tableName.Replace("..", "").Replace("/", "").Replace("\\", "");

            _filePath = $"{safeDbName}_{safeTableName}.db";

            // Initialize Unique Sets for unique columns
            foreach (var col in Schema.Columns) {
                if (col.IsUnique && col.Name != "id") {
                    UniqueIndexes[col.Name] = new HashSet<string>();
                }
            }

            if (!File.Exists(_filePath)) InitFile();
            LoadIndex();
        }

        private void InitFile() {
            using var fs = new FileStream(_filePath, FileMode.Create);
        }

        public void Drop() {
            if (File.Exists(_filePath)) {
                File.Delete(_filePath);
            }
        }

        private void LoadIndex() {
            PrimaryKeyIndex.Clear();
            foreach (var key in UniqueIndexes.Keys) UniqueIndexes[key].Clear();

            if (!File.Exists(_filePath)) return;

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);

            try {
                while (fs.Position < fs.Length) {
                    long currentPos = fs.Position;
                    bool isDeleted = reader.ReadBoolean();
                    int id = reader.ReadInt32();

                    // Read values to populate indices
                    var rowValues = new Dictionary<string, string>();

                    foreach (var col in Schema.Columns) {
                        if (col.Name == "id") continue;

                        if (col.Type == DbType.Int) {
                            var val = reader.ReadInt32().ToString();
                            rowValues[col.Name] = val;
                        }
                        else {
                            var val = reader.ReadString();
                            rowValues[col.Name] = val;
                        }
                    }

                    if (!isDeleted) {
                        PrimaryKeyIndex[id] = currentPos;

                        // Populate Unique Indices
                        foreach (var kvp in UniqueIndexes) {
                            if (rowValues.ContainsKey(kvp.Key)) {
                                kvp.Value.Add(rowValues[kvp.Key]);
                            }
                        }
                    }
                }
            }
            catch (EndOfStreamException) { }
        }

        public void Insert(Row row) {
            if (PrimaryKeyIndex.ContainsKey(row.Id))
                throw new Exception($"Duplicate Primary Key: {row.Id}");

            // Check Unique Constraints
            foreach (var col in Schema.Columns) {
                if (col.IsUnique && col.Name != "id") {
                    var val = row.Data[col.Name].ToString();
                    if (UniqueIndexes.ContainsKey(col.Name) && UniqueIndexes[col.Name].Contains(val!)) {
                        throw new Exception($"Violation of UNIQUE constraint on column '{col.Name}'. Value '{val}' already exists.");
                    }
                }
            }

            using var fs = new FileStream(_filePath, FileMode.Append);
            using var writer = new BinaryWriter(fs);

            long pos = fs.Position;

            writer.Write(false); // IsDeleted
            writer.Write(row.Id);

            foreach (var col in Schema.Columns) {
                if (col.Name == "id") continue;
                if (col.Type == DbType.Int) writer.Write((int)row.Data[col.Name]);
                else writer.Write((string)row.Data[col.Name]);
            }

            PrimaryKeyIndex[row.Id] = pos;

            // Update Unique Indices
            foreach (var col in Schema.Columns) {
                if (col.IsUnique && col.Name != "id") {
                    UniqueIndexes[col.Name].Add(row.Data[col.Name].ToString()!);
                }
            }
        }

        public void Delete(int id) {
            if (!PrimaryKeyIndex.ContainsKey(id))
                throw new Exception($"Record with ID {id} not found.");

            // We need to fetch the row first to remove values from UniqueIndexes
            var row = SelectById(id);
            if (row != null) {
                foreach (var col in Schema.Columns) {
                    if (col.IsUnique && col.Name != "id") {
                        var val = row.Data[col.Name].ToString();
                        if (UniqueIndexes.ContainsKey(col.Name))
                            UniqueIndexes[col.Name].Remove(val!);
                    }
                }
            }

            long offset = PrimaryKeyIndex[id];

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Write);
            using var writer = new BinaryWriter(fs);

            fs.Seek(offset, SeekOrigin.Begin);
            writer.Write(true);

            PrimaryKeyIndex.Remove(id);
        }

        public void Update(Row row) {
            if (!PrimaryKeyIndex.ContainsKey(row.Id))
                throw new Exception($"Record with ID {row.Id} not found.");

            Delete(row.Id);
            Insert(row);
        }

        public Row? SelectById(int id) {
            if (!PrimaryKeyIndex.ContainsKey(id)) return null;

            long offset = PrimaryKeyIndex[id];

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);

            fs.Seek(offset, SeekOrigin.Begin);

            var row = new Row { Data = new Dictionary<string, object>() };
            row.IsDeleted = reader.ReadBoolean();
            if (row.IsDeleted) return null;

            row.Id = reader.ReadInt32();
            row.Data["id"] = row.Id;

            foreach (var col in Schema.Columns) {
                if (col.Name == "id") continue;
                if (col.Type == DbType.Int) row.Data[col.Name] = reader.ReadInt32();
                else row.Data[col.Name] = reader.ReadString();
            }

            return row;
        }

        public List<Row> SelectAll() {
            var rows = new List<Row>();
            if (!File.Exists(_filePath)) return rows;

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);

            while (fs.Position < fs.Length) {
                bool isDeleted = reader.ReadBoolean();
                int id = reader.ReadInt32();
                var row = new Row { Id = id, Data = new Dictionary<string, object>() { { "id", id } } };

                foreach (var col in Schema.Columns) {
                    if (col.Name == "id") continue;
                    if (col.Type == DbType.Int) row.Data[col.Name] = reader.ReadInt32();
                    else row.Data[col.Name] = reader.ReadString();
                }

                if (!isDeleted) rows.Add(row);
            }
            return rows;
        }
    }
}
using System;
using System.Collections.Generic;
using System.IO;

namespace RDBMS.Engine
{
    public class Table
    {
        public TableSchema Schema { get; set; }
        private readonly string _filePath;
        public Dictionary<int, long> PrimaryKeyIndex { get; private set; } = new();

        public Table(string dbName, string tableName, List<ColumnDef> columns)
        {
            Schema = new TableSchema { Name = tableName, Columns = columns };
            var safeDbName = dbName.Replace("..", "").Replace("/", "").Replace("\\", "");
            var safeTableName = tableName.Replace("..", "").Replace("/", "").Replace("\\", "");
            
            _filePath = $"{safeDbName}_{safeTableName}.db"; 
            
            if (!File.Exists(_filePath)) InitFile();
            LoadIndex();
        }

        private void InitFile()
        {
            using var fs = new FileStream(_filePath, FileMode.Create);
        }

        // --- NEW METHOD ---
        public void Drop()
        {
            if (File.Exists(_filePath))
            {
                File.Delete(_filePath);
            }
        }
        // ------------------

        private void LoadIndex()
        {
            PrimaryKeyIndex.Clear();
            if (!File.Exists(_filePath)) return;

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);

            try
            {
                while (fs.Position < fs.Length)
                {
                    long currentPos = fs.Position;
                    bool isDeleted = reader.ReadBoolean();
                    int id = reader.ReadInt32();

                    if (!isDeleted) PrimaryKeyIndex[id] = currentPos;

                    foreach (var col in Schema.Columns)
                    {
                        if (col.Name == "id") continue;
                        if (col.Type == DbType.Int) reader.ReadInt32();
                        else if (col.Type == DbType.String) reader.ReadString();
                    }
                }
            }
            catch (EndOfStreamException) { }
        }

        public void Insert(Row row)
        {
            if (PrimaryKeyIndex.ContainsKey(row.Id))
                throw new Exception($"Duplicate Primary Key: {row.Id}");

            using var fs = new FileStream(_filePath, FileMode.Append);
            using var writer = new BinaryWriter(fs);

            long pos = fs.Position;
            
            writer.Write(false); // IsDeleted
            writer.Write(row.Id);

            foreach (var col in Schema.Columns)
            {
                if (col.Name == "id") continue;
                if (col.Type == DbType.Int) writer.Write((int)row.Data[col.Name]);
                else writer.Write((string)row.Data[col.Name]);
            }

            PrimaryKeyIndex[row.Id] = pos;
        }

        public void Delete(int id)
        {
            if (!PrimaryKeyIndex.ContainsKey(id))
                throw new Exception($"Record with ID {id} not found.");

            long offset = PrimaryKeyIndex[id];

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Write);
            using var writer = new BinaryWriter(fs);

            fs.Seek(offset, SeekOrigin.Begin);
            writer.Write(true); 

            PrimaryKeyIndex.Remove(id);
        }

        public void Update(Row row)
        {
            if (!PrimaryKeyIndex.ContainsKey(row.Id))
                throw new Exception($"Record with ID {row.Id} not found.");

            Delete(row.Id);
            Insert(row);
        }

        public Row? SelectById(int id)
        {
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

            foreach (var col in Schema.Columns)
            {
                if (col.Name == "id") continue;
                if (col.Type == DbType.Int) row.Data[col.Name] = reader.ReadInt32();
                else row.Data[col.Name] = reader.ReadString();
            }

            return row;
        }

        public List<Row> SelectAll()
        {
            var rows = new List<Row>();
            if (!File.Exists(_filePath)) return rows;

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fs);

            while (fs.Position < fs.Length)
            {
                bool isDeleted = reader.ReadBoolean();
                int id = reader.ReadInt32();
                var row = new Row { Id = id, Data = new Dictionary<string, object>() {{ "id", id }} };

                foreach (var col in Schema.Columns)
                {
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
using System;
using System.Collections.Generic;

namespace RDBMS.Engine {
    public enum DbType { Int, String }

    public class ColumnDef {
        public required string Name { get; set; }
        public DbType Type { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsUnique { get; set; }
    }

    public class TableSchema {
        public required string Name { get; set; }
        public List<ColumnDef> Columns { get; set; } = new();
    }
}
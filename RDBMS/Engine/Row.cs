using System.Collections.Generic;

namespace RDBMS.Engine {
    public class Row {
        public int Id { get; set; } // We assume every table has an implicit or explicit Int ID for this challenge
        public Dictionary<string, object> Data { get; set; } = new();
        public bool IsDeleted { get; set; } = false;
    }
}
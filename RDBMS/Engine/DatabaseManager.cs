using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace RDBMS.Engine {
    public class DatabaseManager {
        // Thread-safe dictionary to hold active databases
        private readonly ConcurrentDictionary<string, Database> _databases = new();

        public DatabaseManager() {
            // Initialize default database
            CreateDatabase("default");
        }

        public Database GetDatabase(string name) {
            _databases.TryGetValue(name, out var db);
            return db!;
        }

        public Database CreateDatabase(string name) {
            var newDb = new Database(name);
            _databases[name] = newDb;
            return newDb;
        }

        public List<string> ListDatabases() {
            return _databases.Keys.ToList();
        }
    }
}
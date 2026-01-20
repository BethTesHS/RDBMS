package engine

import "sync"

type DatabaseManager struct {
	Databases map[string]*Database
	mu        sync.RWMutex
}

func NewDatabaseManager() *DatabaseManager {
	mgr := &DatabaseManager{
		Databases: make(map[string]*Database),
	}
	mgr.CreateDatabase("default")
	return mgr
}

func (mgr *DatabaseManager) GetDatabase(name string) *Database {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.Databases[name]
}

func (mgr *DatabaseManager) CreateDatabase(name string) *Database {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	
	if db, exists := mgr.Databases[name]; exists {
		return db
	}
	
	newDb := NewDatabase(name)
	mgr.Databases[name] = newDb
	return newDb
}

func (mgr *DatabaseManager) ListDatabases() []string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	
	keys := make([]string, 0, len(mgr.Databases))
	for k := range mgr.Databases {
		keys = append(keys, k)
	}
	return keys
}
package stats

// StatDB handle connection (safe for concurrent use)
// to ssdbs (currently)
type DB struct {
}

// ConnectStatDB creates ssdb connection
// and returns StatDB
// TODO refactor func signature
func Connect() (*DB, error) {
	var err error
	return &DB{}, err
}

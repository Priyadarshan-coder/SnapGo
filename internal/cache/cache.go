package cache

import "sync"

// Cache is our thread-safe in-memory store.
type Cache struct {
	mu    sync.RWMutex
	store map[string]string
}

// New creates and initializes a new Cache instance.
func New() *Cache {
	return &Cache{
		store: make(map[string]string),
	}
}

// Set stores a value for a given key.
func (c *Cache) Set(key string, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = value
}

// Get retrieves a value for a given key.
// It returns a boolean indicating if the key was found.
func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, exists := c.store[key]
	return value, exists
}

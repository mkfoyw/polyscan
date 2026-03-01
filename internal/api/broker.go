package api

import (
	"encoding/json"
	"sync"
)

// DashEvent represents a real-time event pushed to the dashboard via SSE.
type DashEvent struct {
	Type string      `json:"type"` // "trade", "settlement", "new_market", "price_move"
	Data interface{} `json:"data"`
}

// Broker manages SSE client connections and broadcasts events.
type Broker struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

// NewBroker creates a new SSE broker.
func NewBroker() *Broker {
	return &Broker{
		clients: make(map[chan []byte]struct{}),
	}
}

// Subscribe registers a new client and returns its channel.
func (b *Broker) Subscribe() chan []byte {
	ch := make(chan []byte, 64)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a client and closes its channel.
func (b *Broker) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Publish sends an event to all connected SSE clients.
func (b *Broker) Publish(evt DashEvent) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- payload:
		default:
			// client too slow, skip
		}
	}
}

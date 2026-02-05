package notify

import (
	"sync"

	pb "go-sync/proto"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[string]chan *pb.FileEvent
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string]chan *pb.FileEvent)}
}

func (h *Hub) Register(user string) chan *pb.FileEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan *pb.FileEvent, 32)
	h.clients[user] = ch
	return ch
}

func (h *Hub) Unregister(user string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ch, ok := h.clients[user]; ok {
		close(ch)
		delete(h.clients, user)
	}
}

func (h *Hub) Dispatch(event *pb.FileEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if ch, ok := h.clients[event.FileId]; ok {
		select {
		case ch <- event:
		default:
		}
	}
}
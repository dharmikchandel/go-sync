package events

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	pb "go-sync/proto"
)

type Bus struct {
	conn *nats.Conn
}

func New(url string) (*Bus, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &Bus{conn: nc}, nil
}

func (b *Bus) Publish(event *pb.FileEvent) error {
	data, _ := json.Marshal(event)
	return b.conn.Publish("files.events", data)
}

func (b *Bus) Subscribe(handler func(*pb.FileEvent)) error {
	_, err := b.conn.Subscribe("files.events", func(msg *nats.Msg) {
		var event pb.FileEvent
		json.Unmarshal(msg.Data, &event)
		handler(&event)
	})
	return err
}
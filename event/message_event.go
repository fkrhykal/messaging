package event

import (
	"encoding/json"
	"time"

	"github.com/fkrhykal/messaging/contract"
)

var _ contract.Event = (*MessageCreatedEvent)(nil)
var _ contract.Event = (*MessageSendedEvent)(nil)

var (
	MessageCreated = contract.EventType("message.created")
	MessageSended  = contract.EventType("message.sended")
)

type Message struct {
	SenderID   string `json:"sender_id"`
	ReceiverID string `json:"receiver_id"`
	Body       string `json:"body"`
}

type MessageCreatedEvent struct {
	ID string
	Message
	CreatedAt time.Time `json:"created_at"`
}

// EventID implements Event.
func (m MessageCreatedEvent) EventID() contract.EventID {
	return contract.EventID(m.ID)
}

// EventType implements Event.
func (m MessageCreatedEvent) EventType() contract.EventType {
	return MessageCreated
}

// Payload implements Event.
func (m MessageCreatedEvent) Payload() (contract.Payload, error) {
	payload, err := json.Marshal(m)
	return contract.Payload(payload), err
}

type MessageSendedEvent struct {
	ID string
	Message
	SendedAt time.Time `json:"sended_at"`
}

// EventID implements Event.
func (m MessageSendedEvent) EventID() contract.EventID {
	return contract.EventID(m.ID)
}

// EventType implements Event.
func (m MessageSendedEvent) EventType() contract.EventType {
	return MessageSended
}

// Payload implements Event.
func (m MessageSendedEvent) Payload() (contract.Payload, error) {
	payload, err := json.Marshal(m)
	return contract.Payload(payload), err
}

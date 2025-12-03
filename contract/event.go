package contract

type EventID []byte

func (e EventID) String() string {
	return string(e)
}

type EventType []byte

func (e EventType) String() string {
	return string(e)
}

type Payload []byte

type Event interface {
	EventID() EventID
	Payload() (Payload, error)
	EventType() EventType
}

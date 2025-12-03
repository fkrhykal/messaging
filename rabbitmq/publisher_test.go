package rabbitmq_test

import (
	"testing"
	"time"

	"github.com/fkrhykal/messaging/contract"
	"github.com/fkrhykal/messaging/event"
	"github.com/fkrhykal/messaging/observability"
	"github.com/fkrhykal/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisher(t *testing.T) {
	provider, err := observability.NewTracerProvider(t.Context(), "messaging")
	if err != nil {
		t.Fatalf("failed to initiate tracer provider: %v", err)
	}
	defer provider.Shutdown(t.Context())

	conn, err := amqp.Dial("amqp://localhost:5672")
	if err != nil {
		t.Fatalf("amqp dial failed: %v", err)
	}
	defer conn.Close()

	publisherConfig := &rabbitmq.RabbitMQPublisherConfig{
		Tracer:            provider.Tracer("rabbitmq.publisher"),
		Conn:              conn,
		ExchangeTopicName: "test",
	}

	publisher, err := rabbitmq.NewRabbitMQPublisher[contract.Event](publisherConfig)
	if err != nil {
		t.Fatalf("failed to initiate publisher: %v", err)
	}
	defer publisher.Close()

	err = publisher.Publish(t.Context(), &event.MessageCreatedEvent{
		ID: "1",
		Message: event.Message{
			SenderID:   "1",
			ReceiverID: "2",
			Body:       "body",
		},
		CreatedAt: time.Now(),
	})

	if err != nil {
		t.Fatalf("failed to publish event: %v", err)
	}
}

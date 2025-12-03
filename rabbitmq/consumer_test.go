package rabbitmq_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/fkrhykal/messaging/contract"
	"github.com/fkrhykal/messaging/event"
	"github.com/fkrhykal/messaging/observability"
	"github.com/fkrhykal/messaging/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var exchangeName = "message.events"

func TestConsumer(t *testing.T) {
	pubCtx := context.Background()
	publisherProvider, err := observability.NewTracerProvider(pubCtx, "rabbitmq.messaging_publisher")
	if err != nil {
		t.Fatalf("failed to initiate tracer producer provider: %v", err)
	}
	conCtx := context.Background()

	consumerProvider, err := observability.NewTracerProvider(conCtx, "rabbitmq.messaging_consumer")
	if err != nil {
		t.Fatalf("failed to initiate tracer consumer provider: %v", err)
	}

	conn, err := amqp.Dial("amqp://localhost:5672")
	if err != nil {
		t.Fatalf("amqp dial failed: %v", err)
	}

	publisherConfig := &rabbitmq.RabbitMQPublisherConfig{
		Tracer:            publisherProvider.Tracer("rabbitmq.publisher"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
	}

	publisher, err := rabbitmq.NewRabbitMQPublisher[contract.Event](publisherConfig)
	if err != nil {
		t.Fatalf("failed to initiate publisher: %v", err)
	}

	for i := range 50 {
		messageCreated := &event.MessageCreatedEvent{
			ID: uuid.NewString(),
			Message: event.Message{
				SenderID:   fmt.Sprintf("sender%d", i),
				ReceiverID: fmt.Sprintf("receiver%d", i),
				Body:       fmt.Sprintf("body %d", i),
			},
			CreatedAt: time.Now(),
		}
		if err := publisher.Publish(context.Background(), messageCreated); err != nil {
			t.Fatalf("failed to publish event %s: %v", messageCreated.EventType(), err)
		}
	}

	slog.DebugContext(t.Context(), "")

	logConsumerConfig1 := &rabbitmq.RabbitMQConsumerConfig{
		Tracer:            consumerProvider.Tracer("rabbitmq.consumer.messagecreated.log.1"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
		QueueName:         "log",
		BindingKey:        event.MessageCreated.String(),
	}

	log1, err := rabbitmq.NewRabbitMQConsumer[event.MessageCreatedEvent](logConsumerConfig1)

	if err != nil {
		t.Fatalf("failed to initiate consumer log-1: %v", err)
	}

	logConsumerConfig2 := &rabbitmq.RabbitMQConsumerConfig{
		Tracer:            consumerProvider.Tracer("rabbitmq.consumer.message_created.log.2"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
		QueueName:         "log",
		BindingKey:        event.MessageCreated.String(),
	}

	log2, err := rabbitmq.NewRabbitMQConsumer[event.MessageCreatedEvent](logConsumerConfig2)
	if err != nil {
		t.Fatalf("failed to initiate consumer log-2: %v", err)
	}

	sendMessageConsumerConfig := &rabbitmq.RabbitMQConsumerConfig{
		Tracer:            consumerProvider.Tracer("rabbitmq.consumer.message_created.send_message.1"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
		QueueName:         "send_message",
		BindingKey:        event.MessageCreated.String(),
	}

	sendMessage, err := rabbitmq.NewRabbitMQConsumer[event.MessageCreatedEvent](sendMessageConsumerConfig)
	if err != nil {
		t.Fatalf("failed to initiate consumer 3: %v", err)
	}

	receiveMessageConsumerConfig := &rabbitmq.RabbitMQConsumerConfig{
		Tracer:            consumerProvider.Tracer("rabbitmq.consumer.message_created.receive_message.1"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
		QueueName:         "receive_message",
		BindingKey:        event.MessageSended.String(),
	}

	receiveMessage, err := rabbitmq.NewRabbitMQConsumer[event.MessageSendedEvent](receiveMessageConsumerConfig)
	if err != nil {
		t.Fatalf("failed to initiate consumer 4: %v", err)
	}

	wg := new(sync.WaitGroup)

	wg.Add(50)
	go func() {
		log1.Consume(context.Background(), func(ctx context.Context, event *event.MessageCreatedEvent) error {
			t.Logf("consumer 1 received message created event: %s", event.ID)
			wg.Done()
			return nil
		})
	}()

	go func() {
		log2.Consume(context.Background(), func(ctx context.Context, event *event.MessageCreatedEvent) error {
			t.Logf("consumer 2 received message created event: %s", event.ID)
			wg.Done()
			return nil
		})
	}()

	go func() {
		sendMessage.Consume(context.Background(), func(ctx context.Context, e *event.MessageCreatedEvent) error {
			t.Logf("consumer 3 received message created event: %s", e.ID)
			messageSended := &event.MessageSendedEvent{
				ID:       e.ID,
				Message:  e.Message,
				SendedAt: time.Now(),
			}
			if err := publisher.Publish(ctx, messageSended); err != nil {
				return err
			}
			t.Logf("consumer 3 publish message sended event: %s", messageSended.ID)
			return nil
		})
	}()

	wg.Add(50)

	go func() {
		receiveMessage.Consume(context.Background(), func(ctx context.Context, event *event.MessageSendedEvent) error {
			t.Logf("consumer 4 received message sended event: %s", event.ID)
			wg.Done()
			return nil
		})
	}()

	wg.Wait()

	t.Cleanup(func() {
		conn.Close()
		publisherProvider.Shutdown(pubCtx)
		consumerProvider.Shutdown(conCtx)
	})
}

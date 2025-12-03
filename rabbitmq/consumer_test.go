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
	t.Logf("--- Starting TestConsumer (Flow Corrected) ---")

	// --- SETUP TRACERS AND CONNECTION (STEP 1) ---
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
	t.Logf("Successfully connected to RabbitMQ on port 5672")

	publisherConfig := &rabbitmq.RabbitMQPublisherConfig{
		Tracer:            publisherProvider.Tracer("rabbitmq.publisher"),
		Conn:              conn,
		ExchangeTopicName: exchangeName,
	}

	publisher, err := rabbitmq.NewRabbitMQPublisher[contract.Event](publisherConfig)
	if err != nil {
		t.Fatalf("failed to initiate publisher: %v", err)
	}

	// Queue cleanup from previous run (Crucial for reliable testing)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel for cleanup: %v", err)
	}
	ch.QueueDelete("log", false, false, false)
	ch.QueueDelete("send_message", false, false, false)
	ch.QueueDelete("receive_message", false, false, false)
	t.Logf("Cleaned up previous queues (log, send_message, receive_message).")

	slog.DebugContext(t.Context(), "")

	// --- START CONSUMERS (STEP 2: Consumers must start BEFORE publishing) ---
	wg := new(sync.WaitGroup)
	// Set WaitGroup total: 2 (from log queue) + 2 (from receive_message queue) = 4
	wg.Add(4)
	t.Logf("WaitGroup set to %d. Starting goroutines...", 4)

	// Consumer 1: log1
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
	t.Logf("Initiated Consumer 1 (log1, Queue: log, Key: %s)", event.MessageCreated.String())

	go func() {
		log1.Consume(context.Background(), func(ctx context.Context, event *event.MessageCreatedEvent) error {
			t.Logf("[C1/LOG1] Received MessageCreatedEvent ID: %s. (Calling wg.Done)", event.ID)
			wg.Done()
			return nil
		})
	}()

	// Consumer 2: log2
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
	t.Logf("Initiated Consumer 2 (log2, Queue: log, Key: %s)", event.MessageCreated.String())

	go func() {
		log2.Consume(context.Background(), func(ctx context.Context, event *event.MessageCreatedEvent) error {
			t.Logf("[C2/LOG2] Received MessageCreatedEvent ID: %s. (Calling wg.Done)", event.ID)
			wg.Done()
			return nil
		})
	}()

	// Consumer 3: sendMessage
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
	t.Logf("Initiated Consumer 3 (sendMessage, Queue: send_message, Key: %s)", event.MessageCreated.String())

	go func() {
		sendMessage.Consume(context.Background(), func(ctx context.Context, e *event.MessageCreatedEvent) error {
			t.Logf("[C3/SEND] Received MessageCreatedEvent ID: %s. Creating MessageSendedEvent.", e.ID)
			messageSended := &event.MessageSendedEvent{
				ID:       e.ID,
				Message:  e.Message,
				SendedAt: time.Now(),
			}
			if err := publisher.Publish(ctx, messageSended); err != nil {
				return err
			}
			t.Logf("[C3/SEND] Published MessageSendedEvent ID: %s.", messageSended.ID)
			return nil
		})
	}()

	// Consumer 4: receiveMessage
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
	t.Logf("Initiated Consumer 4 (receiveMessage, Queue: receive_message, Key: %s)", event.MessageSended.String())

	go func() {
		receiveMessage.Consume(context.Background(), func(ctx context.Context, event *event.MessageSendedEvent) error {
			t.Logf("[C4/RECEIVE] Received MessageSendedEvent ID: %s. (Calling wg.Done)", event.ID)
			wg.Done()
			return nil
		})
	}()

	t.Logf("--- Consumers are now active and listening. Starting publishing loop (STEP 3) ---")

	// --- PUBLISH INITIAL MESSAGES HERE (STEP 3) ---
	initialMessageIDs := make([]string, 0)
	for i := range 2 {
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
		initialMessageIDs = append(initialMessageIDs, messageCreated.ID)
		t.Logf("Published MessageCreatedEvent %d ID: %s", i+1, messageCreated.ID)
	}
	t.Logf("Total 2 MessageCreated events published. Initial IDs: %v", initialMessageIDs)

	t.Logf("Waiting for all 4 expected wg.Done() calls...")
	wg.Wait()
	t.Logf("All 4 expected wg.Done() calls received. Test complete.")

	t.Cleanup(func() {
		// Cleanup channel used for deletions
		ch.Close()
		conn.Close()
		publisherProvider.Shutdown(pubCtx)
		consumerProvider.Shutdown(conCtx)
		t.Logf("Final Cleanup complete.")
	})
}

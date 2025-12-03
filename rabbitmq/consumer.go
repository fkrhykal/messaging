package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/fkrhykal/messaging/contract"
	"github.com/fkrhykal/messaging/event"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

var _ contract.Consumer[event.MessageCreatedEvent] = (*rabbitMQConsumer[event.MessageCreatedEvent])(nil)

type rabbitMQConsumer[E contract.Event] struct {
	propagator   propagation.TextMapPropagator
	slogger      *slog.Logger
	tracer       trace.Tracer
	ch           *amqp.Channel
	exchangeName string
	queueName    string
	bindingKey   string
}

func (c *rabbitMQConsumer[E]) Consume(ctx context.Context, handler contract.HandlerFn[E]) error {
	if err := c.ch.Qos(1, 0, false); err != nil {
		return err
	}

	deliveryCh, err := c.ch.Consume(c.queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case delivery, ok := <-deliveryCh:
			if !ok {
				c.slogger.WarnContext(ctx, "delivery channel closed", "queueName", c.queueName, "bindingKey", c.bindingKey)
				return c.ch.Close()
			}
			c.processDelivery(ctx, delivery, handler)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *rabbitMQConsumer[E]) processDelivery(ctx context.Context, delivery amqp.Delivery, handler contract.HandlerFn[E]) {
	carrier := make(propagation.MapCarrier)

	for key, value := range delivery.Headers {
		if v, ok := value.(string); ok {
			carrier.Set(key, v)
		}
	}

	ctx, span := c.tracer.Start(
		c.propagator.Extract(ctx, carrier),
		"consume-event",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemRabbitMQ,
			semconv.MessagingMessageID(delivery.MessageId),
			semconv.MessagingDestinationName(c.queueName),
			semconv.MessagingRabbitMQDestinationRoutingKey(delivery.RoutingKey),
		),
	)
	defer span.End()

	e := new(E)

	if err := json.Unmarshal(delivery.Body, e); err != nil {
		err = fmt.Errorf("failed to unmarshal event: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "unmarshal failed")
		_ = delivery.Nack(false, false)
		return
	}

	if err := handler(ctx, e); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "handler failed")

		if nackErr := delivery.Nack(false, true); nackErr != nil {
			c.slogger.ErrorContext(ctx, "failed to nack message", "error", nackErr)
		}
		return
	}

	if err := delivery.Ack(false); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ack failed")
		c.slogger.ErrorContext(ctx, "failed to ack message", "error", err)
		return
	}

	span.SetStatus(codes.Ok, "processed")
}

func (c *rabbitMQConsumer[E]) Close() error {
	return c.ch.Close()
}

type RabbitMQConsumerConfig struct {
	Tracer            trace.Tracer
	Conn              *amqp.Connection
	ExchangeTopicName string
	QueueName         string
	BindingKey        string
}

func NewRabbitMQConsumer[E contract.Event](config *RabbitMQConsumerConfig) (*rabbitMQConsumer[E], error) {
	ch, err := config.Conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(config.ExchangeTopicName, "topic", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	queue, err := ch.QueueDeclare(config.QueueName, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	if err := ch.QueueBind(queue.Name, config.BindingKey, config.ExchangeTopicName, false, nil); err != nil {
		return nil, err
	}

	return &rabbitMQConsumer[E]{
		slogger:      slog.Default(),
		tracer:       config.Tracer,
		exchangeName: config.ExchangeTopicName,
		queueName:    config.QueueName,
		bindingKey:   config.BindingKey,
		ch:           ch,
		propagator:   propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}, nil
}

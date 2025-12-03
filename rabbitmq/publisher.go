package rabbitmq

import (
	"context"
	"fmt"

	"github.com/fkrhykal/messaging/contract"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

var _ contract.Publisher[contract.Event] = (*rabbitMQPublisher[contract.Event])(nil)

type rabbitMQPublisher[E contract.Event] struct {
	propagator        propagation.TextMapPropagator
	tracer            trace.Tracer
	ch                *amqp.Channel
	exchangeTopicName string
}

func (p *rabbitMQPublisher[E]) Publish(ctx context.Context, event E) error {
	ctx, span := p.tracer.Start(
		ctx,
		"publish-event",
		trace.WithAttributes(
			semconv.MessagingSystemRabbitMQ,
			semconv.MessagingDestinationName(p.exchangeTopicName),
			semconv.MessagingRabbitMQDestinationRoutingKey(event.EventType().String()),
			semconv.MessagingMessageID(event.EventID().String()),
			semconv.MessagingDestinationTemplate("topic"),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	body, err := event.Payload()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "parsing event failed")
		return fmt.Errorf("failed to get payload: %w", err)
	}

	carrier := make(propagation.MapCarrier)
	p.propagator.Inject(ctx, carrier)

	headers := amqp.Table{}
	for k, v := range carrier {
		headers[k] = v
	}

	if err := p.ch.PublishWithContext(
		ctx,
		p.exchangeTopicName,
		event.EventType().String(),
		false,
		false,
		amqp.Publishing{
			Headers:       headers,
			ContentType:   "application/json",
			Body:          body,
			MessageId:     event.EventID().String(),
			Type:          event.EventType().String(),
			CorrelationId: span.SpanContext().TraceID().String(),
			DeliveryMode:  amqp.Persistent,
		},
	); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		return fmt.Errorf("amqp publish failed: %w", err)
	}

	span.SetStatus(codes.Ok, "published")

	return nil
}

func (p *rabbitMQPublisher[E]) Close() error {
	return p.ch.Close()
}

type RabbitMQPublisherConfig struct {
	Tracer            trace.Tracer
	Conn              *amqp.Connection
	ExchangeTopicName string
}

func NewRabbitMQPublisher[E contract.Event](config *RabbitMQPublisherConfig) (*rabbitMQPublisher[E], error) {
	ch, err := config.Conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	if err := ch.ExchangeDeclare(config.ExchangeTopicName, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}

	return &rabbitMQPublisher[E]{
		tracer:            config.Tracer,
		ch:                ch,
		exchangeTopicName: config.ExchangeTopicName,
		propagator:        propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}, nil
}

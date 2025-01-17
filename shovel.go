package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type shovel struct {
	conn *amqp.Connection
}

// TODO: make this configurable
var timeout = time.Second * 15

// NewShovel return a shovel instance using the provided connection
func NewShovel(conn *amqp.Connection) *shovel {
	return &shovel{
		conn: conn,
	}
}

// MoveMessages consume all messages from a queue and republish to other destination exchange within the same broker (or cluster)
func (s *shovel) MoveMessages(source, destination, routingKey string) error {
	err := s.moveMessages(source, destination, routingKey)
	if err != nil {
		return fmt.Errorf("error moving messages: %w", err)
	}

	return nil
}

func (s *shovel) moveMessages(source, destination, routingKey string) error {
	ch, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("error opening new channel: %w", err)
	}
	defer ch.Close()

	for {
		msg, ok, err := ch.Get(source, false)
		if err != nil {
			return errors.Join(fmt.Errorf("error getting delivery from queue: %w", err), msg.Nack(false, true))
		}

		if !ok {
			// no more messages in the queue
			return nil
		}

		pubCh, err := s.conn.Channel()
		if err != nil {
			return fmt.Errorf("error opening new channel: %w", err)
		}
		// confirm mode
		err = pubCh.Confirm(false)
		if err != nil {
			return err
		}

		// republish msg to destination exchange
		err = republish(pubCh, msg.Body, destination, routingKey)
		pubCh.Close()
		if err != nil {
			return errors.Join(err, msg.Nack(false, true))
		}

		err = msg.Ack(false)
		if err != nil {
			return fmt.Errorf("error acking delivery: %w", err)
		}
	}
}

func republish(ch *amqp.Channel, body []byte, destination, routingKey string) error {
	confirmCh := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	undeliveredCh := ch.NotifyReturn(make(chan amqp.Return, 1))
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := ch.PublishWithContext(ctx, destination, routingKey, true, false, amqp.Publishing{Body: body})
	if err != nil {
		return fmt.Errorf("error republishing message: %w", err)
	}

	var undelivered *amqp.Return

	for {
		select {
		case err := <-closeCh:
			return fmt.Errorf("unexpected channel close: %v", err)
		case undeliveredMsg := <-undeliveredCh:
			undelivered = &undeliveredMsg
		case confirm := <-confirmCh:
			if confirm.Ack && undelivered == nil {
				return nil
			} else {
				return fmt.Errorf("mq server has not confirmed the `%v` message: [%v] %v ",
					destination,
					undelivered.ReplyCode,
					undelivered.ReplyText,
				)
			}
		case <-ctx.Done():
			return fmt.Errorf("publish `%v` message timeout", destination)
		}
	}
}

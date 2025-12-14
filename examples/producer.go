package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "test-topic",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	defer w.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("Hello DashMQ! Message %d", i)),
		}

		err := w.WriteMessages(ctx, msg)
		if err != nil {
			log.Fatalf("Failed to write message: %v", err)
		}

		fmt.Printf("✓ Sent message %d: %s\n", i, msg.Value)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\nAll messages sent successfully!")
}


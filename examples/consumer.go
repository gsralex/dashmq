package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "test-topic",
		GroupID:        "test-group",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer r.Close()

	fmt.Println("Waiting for messages...")
	fmt.Println("Press Ctrl+C to stop\n")

	ctx := context.Background()
	messageCount := 0

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("Failed to read message: %v", err)
		}

		messageCount++
		fmt.Printf("[%d] Topic: %s, Partition: %d, Offset: %d\n",
			messageCount, msg.Topic, msg.Partition, msg.Offset)
		fmt.Printf("    Key: %s\n", string(msg.Key))
		fmt.Printf("    Value: %s\n", string(msg.Value))
		fmt.Println()

		if messageCount >= 10 {
			fmt.Println("Received 10 messages, stopping...")
			break
		}
	}
}


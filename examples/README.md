# Examples

This directory contains examples of how to use DashMQ with various Kafka clients.

## Prerequisites

1. Start DashMQ server:
```bash
./dashmq -port 9092
```

2. Install the Kafka client library for your language

## Python Example

```python
from kafka import KafkaProducer, KafkaConsumer
import time

# Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8')
)

for i in range(10):
    producer.send('test-topic', f'Message {i}')
    print(f'Sent message {i}')

producer.flush()
producer.close()

# Consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')
)

print("Waiting for messages...")
for message in consumer:
    print(f"Received: {message.value} at offset {message.offset}")
```

## Go Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/segmentio/kafka-go"
)

func main() {
    // Producer
    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-topic",
    })
    defer w.Close()

    for i := 0; i < 10; i++ {
        err := w.WriteMessages(context.Background(), kafka.Message{
            Value: []byte(fmt.Sprintf("Message %d", i)),
        })
        if err != nil {
            fmt.Printf("Error writing message: %v\n", err)
        } else {
            fmt.Printf("Sent message %d\n", i)
        }
    }

    // Consumer
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-topic",
    })
    defer r.Close()

    fmt.Println("Waiting for messages...")
    for {
        msg, err := r.ReadMessage(context.Background())
        if err != nil {
            break
        }
        fmt.Printf("Received: %s at offset %d\n", string(msg.Value), msg.Offset)
    }
}
```

## Java Example

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;
import java.util.Arrays;

public class Example {
    public static void main(String[] args) {
        // Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("test-topic", "Message " + i));
        }
        producer.close();

        // Consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("test-topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                System.out.println("Received: " + record.value() + " at offset " + record.offset());
            });
        }
    }
}
```



# DashMQ

DashMQ is a Kafka-compatible message queue server written in Go. It implements the core Kafka binary protocol, allowing you to use standard Kafka clients to produce and consume messages.

## Features

- ✅ Kafka binary protocol compatibility
- ✅ Core APIs: Metadata, Produce, Fetch, ListOffsets
- ✅ Topic and partition management
- ✅ In-memory message storage
- ✅ Concurrent client connections
- ✅ Graceful shutdown

## Installation

```bash
git clone https://github.com/dashmq/dashmq.git
cd dashmq
go build -o dashmq
```

## Usage

### Start the server

```bash
./dashmq -host 0.0.0.0 -port 9092
```

Options:
- `-host`: Server host (default: 0.0.0.0)
- `-port`: Server port (default: 9092)
- `-data-dir`: Data directory (default: ./data)
- `-log-level`: Log level (default: info)

### Using with Kafka clients

DashMQ is compatible with standard Kafka clients. You can use any Kafka client library to connect to it.

#### Python example

```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test-topic', b'Hello, DashMQ!')
producer.close()

# Consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers=['localhost:9092'])
for message in consumer:
    print(message.value)
```

#### Go example

```go
package main

import (
    "github.com/segmentio/kafka-go"
)

func main() {
    // Producer
    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-topic",
    })
    w.WriteMessages(context.Background(), kafka.Message{
        Value: []byte("Hello, DashMQ!"),
    })
    w.Close()

    // Consumer
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-topic",
    })
    msg, _ := r.ReadMessage(context.Background())
    fmt.Println(string(msg.Value))
    r.Close()
}
```

## Architecture

```
dashmq/
├── main.go                 # Entry point
├── internal/
│   ├── config/            # Configuration
│   ├── protocol/          # Kafka protocol encoding/decoding
│   ├── storage/           # Message storage (topics, partitions)
│   ├── api/               # API handlers (Metadata, Produce, Fetch, etc.)
│   └── server/            # Server implementation
```

## Supported APIs

- **Metadata (API Key 3)**: Get cluster metadata, topics, and partitions
- **Produce (API Key 0)**: Send messages to topics
- **Fetch (API Key 1)**: Consume messages from topics
- **ListOffsets (API Key 2)**: Get offset information

## Limitations

- In-memory storage (data is lost on restart)
- Single broker (no replication)
- No persistence to disk
- Simplified message format parsing
- No authentication/authorization
- No compression support

## License

Apache License 2.0
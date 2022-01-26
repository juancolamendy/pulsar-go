package main

import (
  "context"
  "fmt"
  "time"
  "log"

  "github.com/apache/pulsar-client-go/pulsar"
)

const (
  ConsumerName = "consumer-01"
)

func main() {
  // get client connection
  client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL:               "pulsar://localhost:6650",
    OperationTimeout:  30 * time.Second,
    ConnectionTimeout: 30 * time.Second,
  })  
  if err != nil {
    log.Fatalf("Could not instantiate Pulsar client. error: %+v", err)
  }
  // close client
  defer client.Close()

  // open a subscription
  consumer, err := client.Subscribe(pulsar.ConsumerOptions{
    Topic:            "persistent://public/default/partitioned-topic",
    SubscriptionName: ConsumerName,
    Type:             pulsar.KeyShared,
  })
  if err != nil {
    log.Fatalf("Could not instantiate consumer. error: %+v", err)
  }
  // close consumer
  defer consumer.Close()

  // processing loop
  for {
    msg, err := consumer.Receive(context.Background())
    if err != nil {
      log.Printf("Failed to receive message. errror: %+v\n", err)
      continue
    }

    // fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
    fmt.Printf("Received message: '%s'\n", string(msg.Payload()))
    consumer.Ack(msg)
  }

  // unsubscribe the consumer
  if err := consumer.Unsubscribe(); err != nil {
    log.Fatal("Failed to unsubscribe. error: %+v", err)
  }  
}

package main

import (
	"context"
	"log"
	"time"
  "fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
  keys = []string{"project_id:project-1", "project_id:project-2", "project_id:project-3"}
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
  
  // create a producer
  producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "persistent://public/default/partitioned-topic",
    Name: "producer01",
  })
  if err != nil {
    log.Fatalf("Could not instantiate a producer. error: %+v\n", err)
  }
  // close producer
  defer producer.Close()

  // processing loop
  for i := 0; i < 5; i++ {
    key := keys[i%3]
    msg := fmt.Sprintf("msg: msg-%d key: %s", i, key)
    _, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
      Payload: []byte(msg),
      Key: key,
    })
    if err != nil {
      log.Printf("Failed to publish message. error: %+v\n", err)
    }
  }
}

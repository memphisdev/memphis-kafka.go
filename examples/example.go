package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/memphisdev/superstream.go"
)

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	brokers := []string{"...", "..."}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.MaxMessages = 10
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// confluent config
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "..."
	config.Net.SASL.Password = "..."
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = nil

	// before every producer/consumer creation you need to call superstream.Init
	config = superstream.Init("token", "superstream-host", config, superstream.Servers(brokers))

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	person1 := Person{Name: "John", Age: 30}
	jsonMsg, err := json.Marshal(person1)
	if err != nil {
		panic(err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.ByteEncoder(jsonMsg),
	})
	if err != nil {
		panic(err)
	}

	config = superstream.Init("token", "superstream-host", config, superstream.Servers(brokers))

	producer2, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	person2 := Person{Name: "Jane", Age: 25}
	jsonMsg, err = json.Marshal(person2)
	if err != nil {
		panic(err)
	}

	_, _, err = producer2.SendMessage(&sarama.ProducerMessage{
		Topic: "test2",
		Value: sarama.ByteEncoder(jsonMsg),
	})
	if err != nil {
		panic(err)
	}

	config = superstream.Init("token", "superstream-host", config, superstream.ConsumerGroup("group"))

	consumer, err := sarama.NewConsumerGroup(brokers, "group", config)
	if err != nil {
		panic(err)
	}

	kafkaHandler := KafkaConsumerGroupHandler{}

	for {
		err := consumer.Consume(context.Background(), []string{"test"}, &kafkaHandler)
		if err != nil {
			panic(err)
		}
	}
}

type KafkaConsumerGroupHandler struct{}

func (h *KafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *KafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *KafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			fmt.Print(string(msg.Value))
			sess.MarkMessage(msg, "")

		case <-sess.Context().Done():
			return nil
		}
	}
}

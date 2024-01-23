package main

import (
	"memphis_kafka"

	"github.com/IBM/sarama"
)

var token = "..."

func main() {

	broker := "..."
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.MaxMessages = 10
	config.Producer.RequiredAcks = sarama.NoResponse
	//config.Producer.Compression = sarama.CompressionZSTD

	// confluent config
	config.Net.SASL.Enable = true //check if this is needed
	config.Net.SASL.User = "..."
	config.Net.SASL.Password = "..."
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = nil

	memphis_kafka.Init(token, memphis_kafka.Host("localhost:4222"))
	memphis_kafka.Start(config)

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("test"),
	})

	if err != nil {
		panic(err)
	}

}

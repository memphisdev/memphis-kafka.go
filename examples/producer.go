package main

import (
	"memphis_kafka"

	"github.com/IBM/sarama"
)

var token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJSTUtUNjRPNUpSNDVKUzY2RTU1SEhCNTJVNDJYMldDRFRGTFdQR1BLWEJEMk1RQjdCR1JRIiwiaWF0IjoxNzA2MDM3NTA1LCJpc3MiOiJBQjZYWFFFWEtVUzM1RTRJT081SUczTUhTUkg0U0JBVkdFTzdZRVlBV0lYR05CSVhYT0oyVUE3VSIsIm5hbWUiOiJyb290Iiwic3ViIjoiVURUR1RXTjJWSUJYUk9INE1JRDJCVjNYN1BSWVFBMlQzMkNKNVZPU01WRDZTSjJNM0ZYUUZIMkEiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyJfSU5CT1guXHUwMDNlIiwibWVtcGhpcy5jbGllbnRUeXBlVXBkYXRlIiwibWVtcGhpcy5yZWdpc3RlckNsaWVudCIsIm1lbXBoaXMuc2NoZW1hLmdldFNjaGVtYSIsIm1lbXBoaXMuc2NoZW1hLmxlYXJuU2NoZW1hLioiLCJtZW1waGlzLnNjaGVtYS5yZWdpc3RlclNjaGVtYS4qIl19LCJzdWIiOnsiYWxsb3ciOlsiX0lOQk9YLlx1MDAzZSIsIm1lbXBoaXMudXBkYXRlcy4qIl19LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.IdFYx7EKIq3u3BMoXYGZSDvnqXX-FLWQMeXNJpVzB9DlRL9Pg3uy58z5FXlA2hBCsy5kdO_DusKnJRJc75o9Aw:::SUAF5CDAAWPFHZBLYG3LG3CXOGY54VY4KKEFIVPXGSKZAP73YNJMCDQYZE"

func main() {

	broker := "pkc-4r087.us-west2.gcp.confluent.cloud:9092"
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.MaxMessages = 10
	config.Producer.RequiredAcks = sarama.NoResponse
	//config.Producer.Compression = sarama.CompressionZSTD

	// confluent config
	config.Net.SASL.Enable = true //check if this is needed
	config.Net.SASL.User = "ZYANRWPEVF3ZK7EZ"
	config.Net.SASL.Password = "F070KfxnwE/UaEBlim8Bt8VtVHlaP4nvvlQyiG8o3tGvmfbeHKwDDC9tp5OouymP"
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

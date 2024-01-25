package main

import (
	"fmt"
	"time"

	memphis_kafka "github.com/memphisdev/memphis-kafka.go"

	"github.com/IBM/sarama"
)

// var token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJDNjREWVA1RlFQWFRPQTNBSERRUExNRVBXWjJWV1AyRjNUMlRDNU9HSURDVVVQUVdZWjdBIiwiaWF0IjoxNzA2MTc3MTQ3LCJpc3MiOiJBQU03TFdNQVhNM0U1RUxaWEVYWEtaR1lXUDJFN081VjRNWlI0MjZON1FETlUyTEc1Q01WREk3SCIsIm5hbWUiOiJyb290Iiwic3ViIjoiVUNDV1ZNSlNRUVo3VFhCU1FRQ0hOUkE2TENPV1JVSTdKU1IzSVRGVE83QVRVTktORVFRNUk2WFQiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyJfSU5CT1guXHUwMDNlIiwibWVtcGhpcy5jbGllbnRUeXBlVXBkYXRlIiwibWVtcGhpcy5yZWdpc3RlckNsaWVudCIsIm1lbXBoaXMuc2NoZW1hLmdldFNjaGVtYSIsIm1lbXBoaXMuc2NoZW1hLmxlYXJuU2NoZW1hLioiLCJtZW1waGlzLnNjaGVtYS5yZWdpc3RlclNjaGVtYS4qIl19LCJzdWIiOnsiYWxsb3ciOlsiJFNZUy5cdTAwM2UiLCJfSU5CT1guXHUwMDNlIiwibWVtcGhpcy51cGRhdGVzLioiXX0sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19.YcqpfqRG9CiVhgoOiitlmH2K6I1Q5JWPoIHiprfopAVgFHiX1OXpsbG90qdynnqSSw62DvBUb5B9mKItUjMSDg:::SUALGUG7VIOFQOC5P5MRN5T3L7UFCGCIDOMVQB6IF5HCQWLEFYPMRGQKTA"
var token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiI3MjNGNEhCWUZSRlg1SVBEUlhHU0xTS0RVS0xGSVJQWTRNSE5NUVo1UENFWVNLVU5EQ1pRIiwiaWF0IjoxNzA2MjA0NjgxLCJpc3MiOiJBQjdQSTNDT1ROMkhBWFFPMzdFQ002T1RIUlRVQVRIRDJVR1JRQzRDUlRRV000WEpNTEpEMldSTSIsIm5hbWUiOiJyb290Iiwic3ViIjoiVUFQTzVUSE1FMlRYWVdQWEZTMkJZVERMWEJKNlBHN1I0UFhCRUZaVUg1T0JWTkVWRkUyN0pTU0wiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyJfSU5CT1guXHUwMDNlIiwibWVtcGhpcy5jbGllbnRUeXBlVXBkYXRlIiwibWVtcGhpcy5yZWdpc3RlckNsaWVudCIsIm1lbXBoaXMuc2NoZW1hLmdldFNjaGVtYSIsIm1lbXBoaXMuc2NoZW1hLmxlYXJuU2NoZW1hLioiLCJtZW1waGlzLnNjaGVtYS5yZWdpc3RlclNjaGVtYS4qIl19LCJzdWIiOnsiYWxsb3ciOlsiX0lOQk9YLlx1MDAzZSIsIm1lbXBoaXMudXBkYXRlcy4qIl19LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.Xaj0T4zDFHl7gASddGTDUnnp30YFHAbspx7lzwATwWsJPrexsds4FLZpACs3ZkpwraODRa0RzsCU0To4oLqHDg:::SUADSSDPJBFUUNTKEFYACGT5Q3X2GTHPVYJMUALOJB2FT3JIRZQJKYNQK4"

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

	err := memphis_kafka.Init(token, memphis_kafka.Host("broker.cost-staging.memphis-gcp.dev:4222"))
	if err != nil {
		fmt.Println(err)
	}
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

	time.Sleep(20 * time.Second)

}

package memphis_kafka

func memphisKafkaErr(msg string) {
	sendClientErrorsToBE(msg)
}

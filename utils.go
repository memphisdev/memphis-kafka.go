package memphis_kafka

import "fmt"

func memphisKafkaErr(msg string) {
	sendClientErrorsToBE(fmt.Sprintf("memphis_kafka client [account name: %v][clientID: %v]: %v", ClientConnection.AccountName, ClientConnection.ClientID, msg))
}

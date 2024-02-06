package superstream

import "fmt"

func handleError(msg string) {
	errMsg := fmt.Sprintf("[account name: %v][clientID: %v][sdk: go][version: %v]%v", ClientConnection.AccountName, ClientConnection.ClientID, sdkVersion, msg)
	sendClientErrorsToBE(errMsg)
}

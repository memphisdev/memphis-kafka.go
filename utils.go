package superstream

import "fmt"

func handleError(msg string) {
	sendClientErrorsToBE(fmt.Sprintf("[account name: %v][clientID: %v]: %v", ClientConnection.AccountName, ClientConnection.ClientID, msg))
}

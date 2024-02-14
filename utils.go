package superstream

import "fmt"

func (c *Client) handleError(msg string) {
	errMsg := fmt.Sprintf("[account name: %v][clientID: %v][sdk: go][version: %v]%v", c.AccountName, c.ClientID, sdkVersion, msg)
	sendClientErrorsToBE(errMsg)
}

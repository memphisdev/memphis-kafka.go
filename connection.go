package superstream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	clientReconnectionUpdateSubject  = "internal.clientReconnectionUpdate"
	clientTypeUpdateSubject          = "internal.clientTypeUpdate"
	clientRegisterSubject            = "internal.registerClient"
	superstreamLearningSubject       = "internal.schema.learnSchema.%v"
	superstreamRegisterSchemaSubject = "internal_tasks.schema.registerSchema.%v"
	superstreamClientUpdatesSubject  = "internal.updates.%v"
	superstreamGetSchemaSubject      = "internal.schema.getSchema.%v"
	superstreamErrorSubject          = "internal.clientErrors"
)

type Option func(*Options) error

type Options struct {
	Host string
}

type RegisterResp struct {
	ClientID       int    `json:"clientId"`
	AccountName    string `json:"accountName"`
	LearningFactor int    `json:"learningFactor"`
}

type RegisterReq struct {
	NatsConnectionID string `json:"natsConnectiontId"`
	Language         string `json:"language"`
	Version          string `json:"version"`
}

type ClientReconnectionUpdateReq struct {
	NewNatsConnectionID string `json:"newNatsConnectiontId"`
	ClientID            int    `json:"clientId"`
}

type ClientTypeUpdateReq struct {
	ClientID int    `json:"clientId"`
	Type     string `json:"type"`
}

type ClientUpdateSub struct {
	ClientID     int
	Subscription *nats.Subscription
	UpdateCahn   chan Update
}

type Update struct {
	Type    string
	Payload []byte
}

type SchemaUpdateReq struct {
	MasterMsgName string
	FileName      string
	SchemaID      string
	Desc          []byte
}

type GetSchemaReq struct {
	SchemaID string `json:"schemaId"`
}

type Client struct {
	ClientID              int
	AccountName           string
	NatsConnectionID      string
	IsConsumer            bool
	IsProducer            bool
	LearningFactor        int
	LearningFactorCounter int
	LearningRequestSent   bool
	GetSchemaRequestSent  bool
	BrokerConnection      *nats.Conn
	JSContext             nats.JetStreamContext
	ProducerProtoDesc     protoreflect.MessageDescriptor
	ProducerSchemaID      string
	ConsumerProtoDescMap  map[string]protoreflect.MessageDescriptor
	ErrorsMsgChan         chan string
}

var ClientConnection *Client

func Init(token string, config interface{}, options ...Option) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				fmt.Println("superstream: error initializing superstream: Wrong option")
				return
			}
		}
	}

	ClientConnection = &Client{}

	err := ClientConnection.InitializeNatsConnection(token, opts.Host)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = ClientConnection.RegisterClient()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = ClientConnection.SubscribeUpdates()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	startInterceptors(config)
	return
}

func Close() {
	ClientConnection.BrokerConnection.Close()
}

func Host(host string) Option {
	return func(o *Options) error {
		o.Host = host
		return nil
	}
}

func GetDefaultOptions() Options {
	return Options{
		Host: "broker.superstream.dev",
	}
}

func (c *Client) InitializeNatsConnection(token, host string) error {

	splitedToken := strings.Split(token, ":::")
	if len(splitedToken) != 2 {
		return fmt.Errorf("superstream: token is not valid")
	}

	JWT := splitedToken[0]
	Nkey := splitedToken[1]

	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1 * time.Second),
		nats.UserJWT(
			func() (string, error) { // Callback to return the user JWT
				return JWT, nil
			},
			func(nonce []byte) ([]byte, error) { // Callback to sign the nonce with user's NKey seed
				userNKey, err := nkeys.FromSeed([]byte(Nkey))
				if err != nil {
					return nil, err
				}
				defer userNKey.Wipe()
				return userNKey.Sign(nonce)
			},
		),
		nats.ReconnectHandler(
			func(nc *nats.Conn) {
				natsConnectionID, err := c.generateNatsConnectionID()
				if err != nil {
					handleError(fmt.Sprintf("[sdk: go][version: %v]InitializeNatsConnection at generateNatsConnectionID: %v", sdkVersion, err.Error()))
				}

				clientReconnectionUpdateReq := ClientReconnectionUpdateReq{
					NewNatsConnectionID: natsConnectionID,
					ClientID:            c.ClientID,
				}

				clientReconnectionUpdateReqBytes, err := json.Marshal(clientReconnectionUpdateReq)
				if err != nil {
					handleError(fmt.Sprintf("[sdk: go][version: %v]InitializeNatsConnection at Marshal %v", sdkVersion, err.Error()))
				}

				_, err = nc.Request(clientReconnectionUpdateSubject, clientReconnectionUpdateReqBytes, 30*time.Second)
				if err != nil {
					handleError(fmt.Sprintf("[sdk: go][version: %v]InitializeNatsConnection at nc.Request %v", sdkVersion, err.Error()))
				}

				c.NatsConnectionID = natsConnectionID
			},
		),
	}

	nc, err := nats.Connect(host, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "nats: maximum account") {
			return fmt.Errorf("superstream: can not connect with superstream since you have reached the maximum amount of connected clients")
		} else if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("superstream: error connecting to superstream: timeout")
		} else if strings.Contains(err.Error(), "unauthorized") {
			return fmt.Errorf("superstream: error connecting to superstream: unauthorized")
		} else {
			return fmt.Errorf("superstream: error connecting to superstream: %v", err)
		}
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("superstream: error connecting to superstream: %v", err)
	}

	c.BrokerConnection = nc
	c.JSContext = js

	natsConnectionID, err := c.generateNatsConnectionID()
	if err != nil {
		return fmt.Errorf("superstream: error connecting to superstream: %v", err)
	}
	c.NatsConnectionID = natsConnectionID

	return nil
}

func (c *Client) RegisterClient() error {
	registerReq := RegisterReq{
		NatsConnectionID: c.NatsConnectionID,
		Language:         "go",
		Version:          sdkVersion,
	}

	registerReqBytes, err := json.Marshal(registerReq)
	if err != nil {
		return fmt.Errorf("superstream: error registering client: %v", err)
	}

	resp, err := c.BrokerConnection.Request(clientRegisterSubject, registerReqBytes, 30*time.Second)
	if err != nil {
		return fmt.Errorf("superstream: error registering client: %v", err)
	}

	var registerResp RegisterResp
	err = json.Unmarshal(resp.Data, &registerResp)
	if err != nil {
		return fmt.Errorf("superstream: error registering client: %v", err)
	}

	c.ClientID = registerResp.ClientID
	c.AccountName = registerResp.AccountName
	c.LearningFactor = registerResp.LearningFactor
	c.LearningFactorCounter = 0
	c.LearningRequestSent = false
	c.GetSchemaRequestSent = false
	c.ConsumerProtoDescMap = make(map[string]protoreflect.MessageDescriptor)
	c.IsConsumer = false
	c.IsProducer = false

	return nil
}

func (c *Client) SubscribeUpdates() error {
	cus := ClientUpdateSub{
		ClientID:   c.ClientID,
		UpdateCahn: make(chan Update),
	}

	go cus.UpdatesHandler()

	var err error
	cus.Subscription, err = c.BrokerConnection.Subscribe(fmt.Sprintf(superstreamClientUpdatesSubject, c.ClientID), cus.SubscriptionHandler())
	if err != nil {
		return fmt.Errorf("superstream: error connecting to superstream %v", err)
	}

	return nil
}

func (c *ClientUpdateSub) UpdatesHandler() {
	for {
		msg := <-c.UpdateCahn
		switch msg.Type {
		case "LearnedSchema":
			var schemaUpdateReq SchemaUpdateReq
			err := json.Unmarshal(msg.Payload, &schemaUpdateReq)
			if err != nil {
				handleError(fmt.Sprintf("[sdk: go][version: %v]UpdatesHandler at json.Unmarshal: %v", sdkVersion, err.Error()))
			}
			desc := compileMsgDescriptor(schemaUpdateReq.Desc, schemaUpdateReq.MasterMsgName, schemaUpdateReq.FileName)
			if desc != nil {
				ClientConnection.ProducerProtoDesc = desc
				ClientConnection.ProducerSchemaID = schemaUpdateReq.SchemaID
			} else {
				handleError(fmt.Sprintf("[sdk: go][version: %v]UpdatesHandler: error compiling schema ", sdkVersion))
			}
		}
	}
}

func (c *ClientUpdateSub) SubscriptionHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update Update
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			handleError(fmt.Sprintf("[sdk: go][version: %v]SubscriptionHandler at json.Unmarshal: %v", sdkVersion, err.Error()))
		}
		c.UpdateCahn <- update
	}
}

func SendLearningMessage(msg []byte) {
	_, err := ClientConnection.JSContext.Publish(fmt.Sprintf(superstreamLearningSubject, ClientConnection.ClientID), msg)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]SendLearningMessage at Publish %v", sdkVersion, err.Error()))
	}
}

func SendRegisterSchemaReq() {
	if ClientConnection.LearningRequestSent {
		return
	}
	_, err := ClientConnection.JSContext.Publish(fmt.Sprintf(superstreamRegisterSchemaSubject, ClientConnection.ClientID), []byte(""))
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]SendRegisterSchemaReq at Publish %v", sdkVersion, err.Error()))
	} else {
		ClientConnection.LearningRequestSent = true
	}
}

func compileMsgDescriptor(desc []byte, MasterMsgName, fileName string) protoreflect.MessageDescriptor {
	descriptorSet := descriptorpb.FileDescriptorSet{}
	err := proto.Unmarshal(desc, &descriptorSet)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at proto.Unmarshal %v", sdkVersion, err.Error()))
		return nil
	}

	localRegistry, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at protodesc.NewFiles %v", sdkVersion, err.Error()))
		return nil
	}

	fileDesc, err := localRegistry.FindFileByPath(fileName)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at FindFileByPath %v", sdkVersion, err.Error()))
		return nil
	}

	msgsDesc := fileDesc.Messages()
	return msgsDesc.ByName(protoreflect.Name(MasterMsgName))
}

func SentGetSchemaRequest(schemaID string) error {
	ClientConnection.GetSchemaRequestSent = true
	req := GetSchemaReq{
		SchemaID: schemaID,
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at json.Marshal %v", sdkVersion, err.Error()))
		ClientConnection.GetSchemaRequestSent = false
		return err
	}

	msg, err := ClientConnection.BrokerConnection.Request(fmt.Sprintf(superstreamGetSchemaSubject, ClientConnection.ClientID), reqBytes, 30*time.Second)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at Request %v", sdkVersion, err.Error()))
		ClientConnection.GetSchemaRequestSent = false
		return err
	}
	var resp SchemaUpdateReq
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor at json.Unmarshal %v", sdkVersion, err.Error()))
		ClientConnection.GetSchemaRequestSent = false
		return err
	}
	desc := compileMsgDescriptor(resp.Desc, resp.MasterMsgName, resp.FileName)
	if desc != nil {
		ClientConnection.ConsumerProtoDescMap[resp.SchemaID] = desc
	} else {
		handleError(fmt.Sprintf("[sdk: go][version: %v]compileMsgDescriptor: error compiling schema", sdkVersion))
		ClientConnection.GetSchemaRequestSent = false
		return fmt.Errorf("superstream: error compiling schema")
	}
	return nil
}

func SendClientTypeUpdateReq(clientID int, clientType string) {
	switch clientType {
	case "consumer":
		ClientConnection.IsConsumer = true
	case "producer":
		ClientConnection.IsProducer = true
	}

	clientTypeUpdateReq := ClientTypeUpdateReq{
		ClientID: clientID,
		Type:     clientType,
	}

	clientTypeUpdateReqBytes, err := json.Marshal(clientTypeUpdateReq)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]SendClientTypeUpdateReq at json.Marshal %v", sdkVersion, err.Error()))
	}

	err = ClientConnection.BrokerConnection.Publish(clientTypeUpdateSubject, clientTypeUpdateReqBytes)
	if err != nil {
		handleError(fmt.Sprintf("[sdk: go][version: %v]SendClientTypeUpdateReq at Publish %v", sdkVersion, err.Error()))
	}
}

func (c *Client) generateNatsConnectionID() (string, error) {
	natsConnectionId, err := c.BrokerConnection.GetClientID()
	if err != nil {
		return "", err
	}

	serverName := c.BrokerConnection.ConnectedServerName()

	return fmt.Sprintf("%v:%v", serverName, natsConnectionId), nil
}

func sendClientErrorsToBE(errMsg string) {
	ClientConnection.BrokerConnection.Publish(superstreamErrorSubject, []byte(errMsg))
}

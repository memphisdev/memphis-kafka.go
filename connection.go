package memphis_kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

type Option func(*Options) error

type Options struct {
	Host string
}

type RegisterResp struct {
	ClientID       int
	LearningFactor int
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
	MsgName  string
	SchemaID string
	Desc     []byte
}

type Client struct {
	ClientID              int
	LearningFactor        int
	LearningFactorCounter int
	LearningRequestSent   bool
	GetSchemaRequestSent  bool
	BrokerConnection      *nats.Conn
	JSContext             nats.JetStreamContext
	ProducerProtoDesc     protoreflect.MessageDescriptor
	ConsumerProtoDescMap  map[string]protoreflect.MessageDescriptor
}

var ClientConnection *Client

func Init(token string, options ...Option) error {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return fmt.Errorf("memphis_kafka: option: %v", err)
			}
		}
	}

	ClientConnection = &Client{}

	err := ClientConnection.InitializeNatsConnection(token, opts.Host)
	if err != nil {
		return err
	}

	err = ClientConnection.RegisterClient()
	if err != nil {
		return err
	}

	err = ClientConnection.SubscribeUpdates()
	if err != nil {
		return err
	}

	return nil
}

func Host(host string) Option {
	return func(o *Options) error {
		o.Host = host
		return nil
	}
}

func GetDefaultOptions() Options {
	return Options{
		Host: "broker.cost.memphis.dev",
	}
}

func (c *Client) InitializeNatsConnection(token, host string) error {
	natsOpts := nats.Options{
		Url:   host,
		Token: token, // change to jwt token
	}

	nc, err := natsOpts.Connect()
	if err != nil {
		return fmt.Errorf("memphis_kafka: error connecting to memphis cost")
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("memphis_kafka: error connecting to memphis cost")
	}

	c.BrokerConnection = nc
	c.JSContext = js

	return nil
}

func (c *Client) RegisterClient() error {
	resp, err := c.BrokerConnection.Request("$memphis.registerClient", []byte(""), 1000)
	if err != nil {
		return fmt.Errorf("memphis_kafka: error registering client")
	}

	var registerResp RegisterResp
	err = json.Unmarshal(resp.Data, &registerResp)
	if err != nil {
		return fmt.Errorf("memphis_kafka: error registering client")
	}

	c.ClientID = registerResp.ClientID
	c.LearningFactor = registerResp.LearningFactor
	c.LearningFactorCounter = 0
	c.LearningRequestSent = false
	c.ConsumerProtoDescMap = make(map[string]protoreflect.MessageDescriptor)

	return nil
}

func (c *Client) SubscribeUpdates() error {
	cus := ClientUpdateSub{
		ClientID:   c.ClientID,
		UpdateCahn: make(chan Update),
	}

	go cus.UpdatesHandler()

	var err error
	cus.Subscription, err = c.BrokerConnection.Subscribe(fmt.Sprintf("$memphis.updates.%v", c.ClientID), cus.SubscriptionHandler())
	if err != nil {
		return fmt.Errorf("memphis_kafka: error connecting to memphis cost")
	}

	return nil
}

func (c *ClientUpdateSub) UpdatesHandler() {
	for {
		msg := <-c.UpdateCahn
		switch msg.Type {
		case "LearnedSchema":
			desc := compileMsgDescriptor(msg.Payload)
			if desc != nil {
				ClientConnection.ProducerProtoDesc = desc
			}
		case "SchemaUpdate":
			desc := compileMsgDescriptor(msg.Payload)
			var update SchemaUpdateReq
			err := json.Unmarshal(msg.Payload, &update)
			if err != nil {
				memphisKafkaErr("error compileing memphis schema")
			}
			if desc != nil {
				ClientConnection.ConsumerProtoDescMap[update.SchemaID] = desc
			}

		}
	}
}

func (c *ClientUpdateSub) SubscriptionHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update Update
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			memphisKafkaErr(fmt.Sprintf("error getting updates from memphis"))
		}
		c.UpdateCahn <- update
	}
}

func SendLearningMessage(msg []byte) {
	_, err := ClientConnection.JSContext.Publish(fmt.Sprintf("$memphis.schema.learnSchema.%v", ClientConnection.ClientID), msg)
	if err != nil {
		memphisKafkaErr("error communicating with memphis")
	}
}

func SendRegisterSchemaReq() {
	//consider using mutexes
	if ClientConnection.LearningRequestSent {
		return
	}
	_, err := ClientConnection.JSContext.Publish(fmt.Sprintf("$memphis.schema.registerSchema.%v", ClientConnection.ClientID), []byte(""))
	if err != nil {
		memphisKafkaErr("error communicating with memphis")
	} else {
		ClientConnection.LearningRequestSent = true
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case <-ticker.C:
					ClientConnection.LearningRequestSent = false
				}
			}
		}()
	}
}

func compileMsgDescriptor(payload []byte) protoreflect.MessageDescriptor {
	var schemaUpdate SchemaUpdateReq
	err := json.Unmarshal(payload, &schemaUpdate)
	if err != nil {
		memphisKafkaErr("error compileing memphis schema")
		return nil
	}

	descriptorSet := descriptorpb.FileDescriptorSet{}
	err = proto.Unmarshal(schemaUpdate.Desc, &descriptorSet)
	if err != nil {
		memphisKafkaErr("error compileing memphis schema")
		return nil
	}

	localRegistry, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		memphisKafkaErr("error compileing memphis schema")
		return nil
	}

	filePath := fmt.Sprintf("%v.proto", "testDescriptor")
	fileDesc, err := localRegistry.FindFileByPath(filePath)
	if err != nil {
		memphisKafkaErr("error compileing memphis schema")
		return nil
	}

	msgsDesc := fileDesc.Messages()
	return msgsDesc.ByName(protoreflect.Name(schemaUpdate.MsgName))
}

func SentGetSchemaRequest(schemaID string) {
	if ClientConnection.GetSchemaRequestSent {
		return
	} else {
		_, err := ClientConnection.JSContext.Publish(fmt.Sprintf("$memphis.schema.getSchema.%v", ClientConnection.ClientID), []byte(schemaID))
		if err != nil {
			memphisKafkaErr("error communicating with memphis")
		}
		ClientConnection.GetSchemaRequestSent = true
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case <-ticker.C:
					ClientConnection.GetSchemaRequestSent = false
				}
			}
		}()
	}
}

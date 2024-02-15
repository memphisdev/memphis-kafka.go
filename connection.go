package superstream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/nats-io/nats.go"
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
	superstreamClientsUpdateSubject  = "internal_tasks.clientsUpdate.%v.%v"
	superstreamInternalUsername      = "superstream_internal"
)

type Option func(*Options) error

type Options struct {
	Host           string
	LearningFactor int
	ConsumerGroup  string
	Servers        string
	Password       string
}

type RegisterResp struct {
	ClientID       int    `json:"client_id"`
	AccountName    string `json:"account_name"`
	LearningFactor int    `json:"learning_factor"`
}

type RegisterReq struct {
	NatsConnectionID string       `json:"nats_connection_id"`
	Language         string       `json:"language"`
	Version          string       `json:"version"`
	LearningFactor   int          `json:"learning_factor"`
	Config           ClientConfig `json:"config"`
}

type ClientReconnectionUpdateReq struct {
	NewNatsConnectionID string `json:"new_nats_connection_id"`
	ClientID            int    `json:"client_id"`
}

type ClientTypeUpdateReq struct {
	ClientID int    `json:"client_id"`
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
	SchemaID string `json:"schema_id"`
}

type ClientConfig struct {
	ClientType                                string             `json:"client_type"`
	ProducerMaxMessageBytes                   int                `json:"producer_max_messages_bytes"`
	ProducerRequiredAcks                      string             `json:"producer_required_acks"`
	ProducerTimeout                           time.Duration      `json:"producer_timeout"`
	ProducerRetryMax                          int                `json:"producer_retry_max"`
	ProducerRetryBackoff                      time.Duration      `json:"producer_retry_backoff"`
	ProducerReturnErrors                      bool               `json:"producer_return_errors"`
	ProducerReturnSuccesses                   bool               `json:"producer_return_successes"`
	ProducerFlushMaxMessages                  int                `json:"producer_flush_max_messages"`
	ProducerCompressionLevel                  string             `json:"producer_compression_level"`
	ConsumerFetchMin                          int32              `json:"consumer_fetch_min"`
	ConsumerFetchDefault                      int32              `json:"consumer_fetch_default"`
	ConsumerRetryBackOff                      time.Duration      `json:"consumer_retry_backoff"`
	ConsumerMaxWaitTime                       time.Duration      `json:"consumer_max_wait_time"`
	ConsumerMaxProcessingTime                 time.Duration      `json:"consumer_mex_processing_time"`
	ConsumerReturnErrors                      bool               `json:"consumer_return_errors"`
	ConsumerOffsetAutoCommitEnable            bool               `json:"consumer_offset_auto_commit_enable"`
	ConsumerOffsetAutoCommintInterval         time.Duration      `json:"consumer_offset_auto_commit_interval"`
	ConsumerOffsetsInitial                    int                `json:"consumer_offsets_initial"`
	ConsumerOffsetsRetryMax                   int                `json:"consumer_offsets_retry_max"`
	ConsumerGroupSessionTimeout               time.Duration      `json:"consumer_group_session_timeout"`
	ConsumerGroupHeartBeatInterval            time.Duration      `json:"consumer_group_heart_beat_interval"`
	ConsumerGroupRebalanceTimeout             time.Duration      `json:"consumer_group_rebalance_timeout"`
	ConsumerGroupRebalanceRetryMax            int                `json:"consumer_group_rebalance_retry_max"`
	ConsumerGroupRebalanceRetryBackOff        time.Duration      `json:"consumer_group_rebalance_retry_back_off"`
	ConsumerGroupRebalanceResetInvalidOffsets bool               `json:"consumer_group_rebalance_reset_invalid_offsets"`
	ConsumerGroupId                           string             `json:"consumer_group_id"`
	Servers                                   string             `json:"servers"`
	ProducerTopicsPartitions                  map[string][]int32 `json:"producer_topics_partitions"`
	ConsumerTopicsPartitions                  map[string][]int32 `json:"consumer_group_topics_partitions"`
}

type TopicsPartitionsPerProducerConsumer struct {
	ProducerTopicsPartitions map[string][]int32 `json:"producer_topics_partitions"`
	ConsumerTopicsPartitions map[string][]int32 `json:"consumer_group_topics_partitions"`
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
	ProducerProtoDesc     protoreflect.MessageDescriptor
	ProducerSchemaID      string
	ConsumerProtoDescMap  map[string]protoreflect.MessageDescriptor
	Counters              ClientCounters
	Config                ClientConfig
}

var BrokerConnection *nats.Conn
var JSContext nats.JetStreamContext
var NatsConnectionID string

type ClientCounters struct {
	TotalBytesBeforeReduction         int64 `json:"total_bytes_before_reduction"`
	TotalBytesAfterReduction          int64 `json:"total_bytes_after_reduction"`
	TotalMessagesSuccessfullyProduce  int   `json:"total_messages_successfully_produce"`
	TotalMessagesSuccessfullyConsumed int   `json:"total_messages_successfully_consumed"`
	TotalMessagesFailedProduce        int   `json:"total_messages_failed_produce"`
	TotalMessagesFailedConsume        int   `json:"total_messages_failed_consume"`
}

var Clients map[int]*Client

func ConfigHandler(clientType string, config *sarama.Config) ClientConfig {
	producerConfig := config.Producer
	consumerConfig := config.Consumer

	var requiredAcks string

	switch config.Producer.RequiredAcks {
	case 0:
		requiredAcks = "NoResponse"
	case 1:
		requiredAcks = "WaitForLocal"
	case -1:
		requiredAcks = "WaitForAll"
	}

	var compressionLevel string
	switch config.Producer.CompressionLevel {
	case 0:
		compressionLevel = "CompressionNone"
	case 1:
		compressionLevel = "CompressionGZIP"
	case 2:
		compressionLevel = "CompressionSnappy"
	case 3:
		compressionLevel = "CompressionZSTD"
	case 0x08:
		compressionLevel = "compressionCodecMask"
	case 5:
		compressionLevel = "timestampTypeMask"
	case -1000:
		compressionLevel = "CompressionLevelDefault"
	default:
		compressionLevel = "CompressionLevelDefault"
	}
	conf := ClientConfig{
		ClientType:                                clientType,
		ProducerMaxMessageBytes:                   producerConfig.MaxMessageBytes,
		ProducerRequiredAcks:                      requiredAcks,
		ProducerTimeout:                           producerConfig.Timeout,
		ProducerRetryMax:                          producerConfig.Retry.Max,
		ProducerRetryBackoff:                      producerConfig.Retry.Backoff,
		ProducerReturnErrors:                      producerConfig.Return.Errors,
		ProducerReturnSuccesses:                   producerConfig.Return.Successes,
		ProducerFlushMaxMessages:                  producerConfig.Flush.MaxMessages,
		ProducerCompressionLevel:                  compressionLevel,
		ConsumerFetchMin:                          consumerConfig.Fetch.Min,
		ConsumerFetchDefault:                      consumerConfig.Fetch.Default,
		ConsumerRetryBackOff:                      consumerConfig.Retry.Backoff,
		ConsumerMaxWaitTime:                       consumerConfig.MaxWaitTime,
		ConsumerMaxProcessingTime:                 consumerConfig.MaxProcessingTime,
		ConsumerReturnErrors:                      consumerConfig.Return.Errors,
		ConsumerOffsetAutoCommitEnable:            consumerConfig.Offsets.AutoCommit.Enable,
		ConsumerOffsetAutoCommintInterval:         consumerConfig.Offsets.AutoCommit.Interval,
		ConsumerOffsetsInitial:                    int(consumerConfig.Offsets.Initial),
		ConsumerOffsetsRetryMax:                   consumerConfig.Offsets.Retry.Max,
		ConsumerGroupSessionTimeout:               consumerConfig.Group.Session.Timeout,
		ConsumerGroupHeartBeatInterval:            consumerConfig.Group.Heartbeat.Interval,
		ConsumerGroupRebalanceTimeout:             consumerConfig.Group.Rebalance.Timeout,
		ConsumerGroupRebalanceRetryMax:            consumerConfig.Group.Rebalance.Retry.Max,
		ConsumerGroupRebalanceRetryBackOff:        consumerConfig.Group.Rebalance.Retry.Backoff,
		ConsumerGroupRebalanceResetInvalidOffsets: consumerConfig.Group.ResetInvalidOffsets,
	}

	return conf
}

func Init(token string, config interface{}, options ...Option) *sarama.Config {

	sconfig := config.(*sarama.Config)
	newConfig := *sconfig

	if Clients == nil {
		Clients = make(map[int]*Client)
	}

	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				fmt.Printf("superstream: error initializing superstream: Wrong option: %s", err.Error())
				return &newConfig
			}
		}
	}
	var clientType string
	if _, ok := config.(*sarama.Config); ok {
		clientType = "kafka"
	}

	conf := ConfigHandler(clientType, config.(*sarama.Config))
	newClient := &Client{Config: conf}

	if BrokerConnection == nil {
		err := InitializeNatsConnection(token, opts.Host)
		if err != nil {
			fmt.Println("superstream: ", err.Error())
			return &newConfig
		}
	}

	newClient.LearningFactor = opts.LearningFactor
	newClient.Config.Servers = opts.Servers
	newClient.Config.ConsumerGroupId = opts.ConsumerGroup
	err := newClient.RegisterClient()
	if err != nil {
		fmt.Println("superstream: ", err.Error())
		return &newConfig
	}

	Clients[newClient.ClientID] = newClient

	err = newClient.SubscribeUpdates()
	if err != nil {
		fmt.Println("superstream: ", err.Error())
		return &newConfig
	}

	go newClient.reportClientsUpdate()

	startInterceptors(&newConfig, newClient)
	return &newConfig
}

func Close() {
	BrokerConnection.Close()
}

func Host(host string) Option {
	return func(o *Options) error {
		o.Host = host
		return nil
	}
}

func ConsumerGroup(consumerGroup string) Option {
	return func(o *Options) error {
		o.ConsumerGroup = consumerGroup
		return nil
	}
}

func Servers(servers string) Option {
	return func(o *Options) error {
		o.Servers = servers
		return nil
	}
}

func LearningFactor(learningFactor int) Option {
	return func(o *Options) error {
		if learningFactor >= 0 && learningFactor <= 10000 {
			o.LearningFactor = learningFactor
			return nil
		} else {
			return fmt.Errorf("learning factor should be in range of 0 to 10000")
		}
	}
}

func Password(password string) Option {
	return func(o *Options) error {
		o.Password = password
		return nil
	}
}

func GetDefaultOptions() Options {
	return Options{
		Host: "broker.superstream.dev",
	}
}

func InitializeNatsConnection(password, host string) error {

	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.Timeout(30 * time.Second),
		nats.ReconnectWait(1 * time.Second),
		nats.UserInfo(
			superstreamInternalUsername,
			password,
		),
		nats.ReconnectHandler(
			func(nc *nats.Conn) {
				var natsConnectionID string
				for _, c := range Clients {
					natsConnectionID, err := generateNatsConnectionID()
					if err != nil {
						c.handleError(fmt.Sprintf(" InitializeNatsConnection at generateNatsConnectionID: %v", err.Error()))
						return
					}

					clientReconnectionUpdateReq := ClientReconnectionUpdateReq{
						NewNatsConnectionID: natsConnectionID,
						ClientID:            c.ClientID,
					}

					clientReconnectionUpdateReqBytes, err := json.Marshal(clientReconnectionUpdateReq)
					if err != nil {
						c.handleError(fmt.Sprintf(" InitializeNatsConnection at Marshal %v", err.Error()))
						return
					}

					_, err = nc.Request(clientReconnectionUpdateSubject, clientReconnectionUpdateReqBytes, 30*time.Second)
					if err != nil {
						c.handleError(fmt.Sprintf(" InitializeNatsConnection at nc.Request %v", err.Error()))
						return
					}
				}

				NatsConnectionID = natsConnectionID
			},
		),
	}

	nc, err := nats.Connect(host, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "nats: maximum account") {
			return fmt.Errorf("can not connect with superstream since you have reached the maximum amount of connected clients")
		} else if strings.Contains(err.Error(), "invalid checksum") {
			return fmt.Errorf("error connecting with superstream: unauthorized")
		} else {
			return fmt.Errorf("error connecting with superstream: %v", err)
		}
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("superstream: error connecting with superstream: %v", err)
	}

	BrokerConnection = nc
	JSContext = js

	natsConnectionID, err := generateNatsConnectionID()
	if err != nil {
		return fmt.Errorf("superstream: error connecting with superstream: %v", err)
	}
	NatsConnectionID = natsConnectionID

	return nil
}

func (c *Client) RegisterClient() error {
	registerReq := RegisterReq{
		NatsConnectionID: NatsConnectionID,
		Language:         "go",
		Version:          sdkVersion,
		LearningFactor:   c.LearningFactor,
		Config:           c.Config,
	}

	registerReqBytes, err := json.Marshal(registerReq)
	if err != nil {
		return fmt.Errorf("superstream: error registering client: %v", err)
	}

	resp, err := BrokerConnection.Request(clientRegisterSubject, registerReqBytes, 30*time.Second)
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
	c.Counters = ClientCounters{
		TotalBytesBeforeReduction:         0,
		TotalBytesAfterReduction:          0,
		TotalMessagesSuccessfullyProduce:  0,
		TotalMessagesSuccessfullyConsumed: 0,
		TotalMessagesFailedProduce:        0,
		TotalMessagesFailedConsume:        0,
	}

	return nil
}

func (c *Client) SubscribeUpdates() error {
	cus := ClientUpdateSub{
		ClientID:   c.ClientID,
		UpdateCahn: make(chan Update),
	}

	go cus.UpdatesHandler()

	var err error
	cus.Subscription, err = BrokerConnection.Subscribe(fmt.Sprintf(superstreamClientUpdatesSubject, c.ClientID), cus.SubscriptionHandler())
	if err != nil {
		return fmt.Errorf("superstream: error connecting with superstream %v", err)
	}

	return nil
}

func (c *ClientUpdateSub) UpdatesHandler() {
	for {
		msg := <-c.UpdateCahn
		switch msg.Type {
		case "LearnedSchema":
			if client, ok := Clients[c.ClientID]; ok {
				var schemaUpdateReq SchemaUpdateReq
				err := json.Unmarshal(msg.Payload, &schemaUpdateReq)
				if err != nil {
					client.handleError(fmt.Sprintf(" UpdatesHandler at json.Unmarshal: %v", err.Error()))
				}
				desc := client.compileMsgDescriptor(schemaUpdateReq.Desc, schemaUpdateReq.MasterMsgName, schemaUpdateReq.FileName)
				if desc != nil {
					client.ProducerProtoDesc = desc
					client.ProducerSchemaID = schemaUpdateReq.SchemaID

				} else {
					client.handleError(fmt.Sprintf("UpdatesHandler: error compiling schema"))
				}
			}
		}
	}
}

func (c *ClientUpdateSub) SubscriptionHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update Update
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			Clients[c.ClientID].handleError(fmt.Sprintf(" SubscriptionHandler at json.Unmarshal: %v", err.Error()))
		}
		c.UpdateCahn <- update
	}
}

func (c *Client) SendLearningMessage(msg []byte) {
	_, err := JSContext.Publish(fmt.Sprintf(superstreamLearningSubject, c.ClientID), msg)
	if err != nil {
		c.handleError(fmt.Sprintf(" SendLearningMessage at Publish %v", err.Error()))
	}
}

func (c *Client) SendRegisterSchemaReq() {
	if c.LearningRequestSent {
		return
	}
	_, err := JSContext.Publish(fmt.Sprintf(superstreamRegisterSchemaSubject, c.ClientID), []byte(""))
	if err != nil {
		c.handleError(fmt.Sprintf(" SendRegisterSchemaReq at Publish %v", err.Error()))
	} else {
		c.LearningRequestSent = true
	}
}

func (c *Client) compileMsgDescriptor(desc []byte, MasterMsgName, fileName string) protoreflect.MessageDescriptor {
	descriptorSet := descriptorpb.FileDescriptorSet{}
	err := proto.Unmarshal(desc, &descriptorSet)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at proto.Unmarshal %v", err.Error()))
		return nil
	}

	localRegistry, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at protodesc.NewFiles %v", err.Error()))
		return nil
	}

	fileDesc, err := localRegistry.FindFileByPath(fileName)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at FindFileByPath %v", err.Error()))
		return nil
	}

	msgsDesc := fileDesc.Messages()
	return msgsDesc.ByName(protoreflect.Name(MasterMsgName))
}

func (c *Client) SentGetSchemaRequest(schemaID string) error {
	c.GetSchemaRequestSent = true
	req := GetSchemaReq{
		SchemaID: schemaID,
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at json.Marshal %v", err.Error()))
		c.GetSchemaRequestSent = false
		return err
	}

	msg, err := BrokerConnection.Request(fmt.Sprintf(superstreamGetSchemaSubject, c.ClientID), reqBytes, 30*time.Second)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at Request %v", err.Error()))
		c.GetSchemaRequestSent = false
		return err
	}
	var resp SchemaUpdateReq
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor at json.Unmarshal %v", err.Error()))
		c.GetSchemaRequestSent = false
		return err
	}
	desc := c.compileMsgDescriptor(resp.Desc, resp.MasterMsgName, resp.FileName)
	if desc != nil {
		c.ConsumerProtoDescMap[resp.SchemaID] = desc
	} else {
		c.handleError(fmt.Sprintf(" compileMsgDescriptor: error compiling schema"))
		c.GetSchemaRequestSent = false
		return fmt.Errorf("superstream: error compiling schema")
	}
	return nil
}

func (c *Client) SendClientTypeUpdateReq(clientType string) {
	switch clientType {
	case "consumer":
		c.IsConsumer = true
	case "producer":
		c.IsProducer = true
	}

	clientTypeUpdateReq := ClientTypeUpdateReq{
		ClientID: c.ClientID,
		Type:     clientType,
	}

	clientTypeUpdateReqBytes, err := json.Marshal(clientTypeUpdateReq)
	if err != nil {
		c.handleError(fmt.Sprintf(" SendClientTypeUpdateReq at json.Marshal %v", err.Error()))
	}

	err = BrokerConnection.Publish(clientTypeUpdateSubject, clientTypeUpdateReqBytes)
	if err != nil {
		c.handleError(fmt.Sprintf(" SendClientTypeUpdateReq at Publish %v", err.Error()))
	}
}

func generateNatsConnectionID() (string, error) {
	natsConnectionId, err := BrokerConnection.GetClientID()
	if err != nil {
		return "", err
	}

	serverName := BrokerConnection.ConnectedServerName()

	return fmt.Sprintf("%v:%v", serverName, natsConnectionId), nil
}

func sendClientErrorsToBE(errMsg string) {
	BrokerConnection.Publish(superstreamErrorSubject, []byte(errMsg))
}

func (c *Client) reportClientsUpdate() {
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ticker.C:
			byteCounters, err := json.Marshal(c.Counters)
			if err != nil {
				c.handleError(fmt.Sprintf("reportClientsUpdate at json.Marshal %v", err.Error()))
			}

			if c.Config.ConsumerTopicsPartitions == nil {
				c.Config.ConsumerTopicsPartitions = map[string][]int32{}
			}

			topicPartitionConfig := TopicsPartitionsPerProducerConsumer{
				ProducerTopicsPartitions: c.Config.ProducerTopicsPartitions,
				ConsumerTopicsPartitions: c.Config.ConsumerTopicsPartitions,
			}

			byteConfig, err := json.Marshal(topicPartitionConfig)
			if err != nil {
				c.handleError(fmt.Sprintf("reportClientsUpdate at json.Marshal %v", err.Error()))
			}

			err = BrokerConnection.Publish(fmt.Sprintf(superstreamClientsUpdateSubject, "counters", c.ClientID), byteCounters)
			if err != nil {
				c.handleError(fmt.Sprintf("reportClientsUpdate at Publish %v to counters subject", err.Error()))
			}

			err = BrokerConnection.Publish(fmt.Sprintf(superstreamClientsUpdateSubject, "config", c.ClientID), byteConfig)
			if err != nil {
				c.handleError(fmt.Sprintf("reportClientsUpdate at Publish to config subject %v", err.Error()))
			}
		}
	}
}

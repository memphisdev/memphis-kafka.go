package superstream

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type SaramaProducerInterceptor struct{}
type SaramaConsumerInterceptor struct{}

func ConfigSaramaInterceptor(config *sarama.Config) {
	if config.Producer.Interceptors != nil {
		config.Producer.Interceptors = append(config.Producer.Interceptors, &SaramaProducerInterceptor{})
	} else {
		config.Producer.Interceptors = []sarama.ProducerInterceptor{&SaramaProducerInterceptor{}}
	}

	if config.Consumer.Interceptors != nil {
		config.Consumer.Interceptors = append(config.Consumer.Interceptors, &SaramaConsumerInterceptor{})
	} else {
		config.Consumer.Interceptors = []sarama.ConsumerInterceptor{&SaramaConsumerInterceptor{}}
	}
}

func (s *SaramaProducerInterceptor) OnSend(msg *sarama.ProducerMessage) {
	if ClientConnection.Config.ProducerTopicsPartitions == nil {
		ClientConnection.Config.ProducerTopicsPartitions = map[string]int32{}
	}
	ClientConnection.Config.ProducerTopicsPartitions[msg.Topic] = msg.Partition
	if !ClientConnection.IsProducer {
		SendClientTypeUpdateReq(ClientConnection.ClientID, "producer")
	}

	byte_msg, err := msg.Value.Encode()
	if err != nil {
		handleError(fmt.Sprintf(" OnSend at msg.Value.Encode %v", err.Error()))
		return
	}

	ClientConnection.Counters.TotalBytesBeforeReduction += int64(len(byte_msg))

	if ClientConnection.ProducerProtoDesc != nil {
		protoMsg, err := jsonToProto(byte_msg)
		if err != nil {
			// in case of a schema mismatch, send the message as is
			ClientConnection.Counters.TotalBytesAfterReduction += int64(len(byte_msg))
			ClientConnection.Counters.TotalMessagesFailedProduce++
			return
		} else {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte("superstream_schema"),
				Value: []byte(ClientConnection.ProducerSchemaID),
			})
			ClientConnection.Counters.TotalBytesAfterReduction += int64(len(protoMsg))
			ClientConnection.Counters.TotalMessagesSuccessfullyProduce++
			msg.Value = sarama.ByteEncoder(protoMsg)
		}
	} else {
		ClientConnection.Counters.TotalBytesAfterReduction += int64(len(byte_msg))
		ClientConnection.Counters.TotalMessagesFailedProduce++
		if ClientConnection.LearningFactorCounter <= ClientConnection.LearningFactor {
			SendLearningMessage(byte_msg)
			ClientConnection.LearningFactorCounter++
		} else if !ClientConnection.LearningRequestSent && ClientConnection.LearningFactorCounter >= ClientConnection.LearningFactor && ClientConnection.ProducerProtoDesc == nil {
			SendRegisterSchemaReq()
		}
	}
}

func (s *SaramaConsumerInterceptor) OnConsume(msg *sarama.ConsumerMessage) {
	if !ClientConnection.IsConsumer {
		SendClientTypeUpdateReq(ClientConnection.ClientID, "consumer")
	}

	if ClientConnection.Config.ConsumerTopicsPartitions == nil {
		ClientConnection.Config.ConsumerTopicsPartitions = map[string]int32{}
	}

	ClientConnection.Config.ConsumerTopicsPartitions[msg.Topic] = msg.Partition
	ClientConnection.Counters.TotalBytesAfterReduction += int64(len(msg.Value))

	for i, header := range msg.Headers {
		if string(header.Key) == "superstream_schema" {
			schemaID := string(header.Value)
			_, ok := ClientConnection.ConsumerProtoDescMap[schemaID]
			if !ok {
				if !ClientConnection.GetSchemaRequestSent {
					SentGetSchemaRequest(schemaID)
				}

				for !ok {
					time.Sleep(500 * time.Millisecond)
					_, ok = ClientConnection.ConsumerProtoDescMap[schemaID]
				}
			}

			descriptor, ok := ClientConnection.ConsumerProtoDescMap[schemaID]
			if ok {
				jsonMsg, err := protoToJson(msg.Value, descriptor)
				if err != nil {
					//Print error
					handleError(fmt.Sprintf(" OnConsume at protoToJson %v", err.Error()))
					return
				} else {
					msg.Headers = append(msg.Headers[:i], msg.Headers[i+1:]...)
					msg.Value = jsonMsg
					ClientConnection.Counters.TotalBytesBeforeReduction += int64(len(jsonMsg))
					ClientConnection.Counters.TotalMessagesSuccessfullyConsumed++
				}
			} else {
				handleError(fmt.Sprintf(" OnConsume schema not found"))
				fmt.Println("superstream: schema not found")
				return
			}
			return
		}
	}
	ClientConnection.Counters.TotalBytesBeforeReduction += int64(len(msg.Value))
	ClientConnection.Counters.TotalMessagesFailedConsume++
}

package superstream

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type SaramaProducerInterceptor struct{}
type SaramaConsumerInterceptor struct{}

func ConfigSaramaInterceptor(config *sarama.Config) {
	config.Producer.Interceptors = []sarama.ProducerInterceptor{&SaramaProducerInterceptor{}}
	config.Consumer.Interceptors = []sarama.ConsumerInterceptor{&SaramaConsumerInterceptor{}}
}

func (s *SaramaProducerInterceptor) OnSend(msg *sarama.ProducerMessage) {
	if !ClientConnection.IsProducer {
		SendClientTypeUpdateReq(ClientConnection.ClientID, "producer")
	}

	if ClientConnection.ProducerProtoDesc != nil {
		byte_msg, err := msg.Value.Encode()
		if err != nil {
			handleError(fmt.Sprintf("[sdk: go][version: %v]OnSend at msg.Value.Encode %v", sdkVersion, err.Error()))
			return
		}
		protoMsg, err := jsonToProto(byte_msg)
		if err != nil {
			// in case of a schema mismatch, send the message as is
			return
		} else {
			if err != nil {
				handleError(fmt.Sprintf("[sdk: go][version: %v]OnSend at binary.Write %v", sdkVersion, err.Error()))
				return
			}
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte("superstream_schema"),
				Value: []byte(ClientConnection.ProducerSchemaID),
			})
			msg.Value = sarama.ByteEncoder(protoMsg)
		}
	} else {
		if ClientConnection.LearningFactorCounter <= ClientConnection.LearningFactor {
			byte_msg, err := msg.Value.Encode()
			if err != nil {
				handleError(fmt.Sprintf("[sdk: go][version: %v]OnSend at msg.Value.Encode %v", sdkVersion, err.Error()))
				return
			}
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
					handleError(fmt.Sprintf("[sdk: go][version: %v]OnConsume at protoToJson %v", sdkVersion, err.Error()))
					return
				} else {
					msg.Headers = append(msg.Headers[:i], msg.Headers[i+1:]...)
					msg.Value = jsonMsg
				}
			} else {
				handleError(fmt.Sprintf("[sdk: go][version: %v]OnConsume schema not found", sdkVersion))
				fmt.Println("superstream: schema not found")
				return
			}
			break
		}
	}
}

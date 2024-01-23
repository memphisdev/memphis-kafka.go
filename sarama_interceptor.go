package memphis_kafka

import (
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
			memphisKafkaErr("error encoding message")
			return
		}
		protoMsg, err := jsonToProto(byte_msg)
		if err != nil {
			return
		} else {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte("memphis_schema"),
				Value: []byte(ClientConnection.ProducerProtoDesc.FullName()), // change the value ?
			})
			msg.Value = sarama.ByteEncoder(protoMsg)
		}
	} else {
		if ClientConnection.LearningFactorCounter <= ClientConnection.LearningFactor {
			byte_msg, err := msg.Value.Encode()
			if err != nil {
				memphisKafkaErr("error encoding message")
				return
			}
			SendLearningMessage(byte_msg)
			ClientConnection.LearningFactorCounter++
		} else if !ClientConnection.LearningRequestSent && ClientConnection.LearningFactorCounter > ClientConnection.LearningFactor {
			SendRegisterSchemaReq()
		}
	}
}

func (s *SaramaConsumerInterceptor) OnConsume(msg *sarama.ConsumerMessage) {
	if !ClientConnection.IsConsumer {
		SendClientTypeUpdateReq(ClientConnection.ClientID, "consumer")
	}

	for i, header := range msg.Headers {
		if string(header.Key) == "memphis_schema" {
			_, ok := ClientConnection.ConsumerProtoDescMap[string(header.Value)]
			if !ok {
				SentGetSchemaRequest(string(header.Value))
				for {
					if _, ok := ClientConnection.ConsumerProtoDescMap[string(header.Value)]; ok {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			descriptor, ok := ClientConnection.ConsumerProtoDescMap[string(header.Value)]
			if ok {
				jsonMsg, err := protoToJson(msg.Value, descriptor)
				if err != nil {
					memphisKafkaErr("error decoding message")
					return
				} else {
					msg.Headers = append(msg.Headers[:i], msg.Headers[i+1:]...)
					msg.Value = jsonMsg
				}
			}
		}
	}
}

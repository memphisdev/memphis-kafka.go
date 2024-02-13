package superstream

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type SaramaProducerInterceptor struct {
	Client *Client
}
type SaramaConsumerInterceptor struct {
	Client *Client
}

func ConfigSaramaInterceptor(config *sarama.Config, client *Client) {
	if config.Producer.Interceptors != nil {
		for i, interceptor := range config.Producer.Interceptors {
			if _, ok := interceptor.(*SaramaProducerInterceptor); ok {
				config.Producer.Interceptors = append(config.Producer.Interceptors[:i], config.Producer.Interceptors[i+1:]...)
				break
			}
		}

		config.Producer.Interceptors = append(config.Producer.Interceptors, &SaramaProducerInterceptor{
			Client: client,
		})
	} else {
		config.Producer.Interceptors = []sarama.ProducerInterceptor{&SaramaProducerInterceptor{
			Client: client,
		}}
	}

	if config.Consumer.Interceptors != nil {
		for i, interceptor := range config.Consumer.Interceptors {
			if _, ok := interceptor.(*SaramaConsumerInterceptor); ok {
				config.Consumer.Interceptors = append(config.Consumer.Interceptors[:i], config.Consumer.Interceptors[i+1:]...)
				break
			}
		}

		config.Consumer.Interceptors = append(config.Consumer.Interceptors, &SaramaConsumerInterceptor{
			Client: client,
		})
	} else {
		config.Consumer.Interceptors = []sarama.ConsumerInterceptor{&SaramaConsumerInterceptor{
			Client: client,
		}}
	}
}

func (s *SaramaProducerInterceptor) OnSend(msg *sarama.ProducerMessage) {
	if s.Client.Config.ProducerTopicsPartitions == nil {
		s.Client.Config.ProducerTopicsPartitions = map[string]int32{}
	}
	s.Client.Config.ProducerTopicsPartitions[msg.Topic] = msg.Partition
	if !s.Client.IsProducer {
		s.Client.SendClientTypeUpdateReq("producer")
	}

	byte_msg, err := msg.Value.Encode()
	if err != nil {
		s.Client.handleError(fmt.Sprintf(" OnSend at msg.Value.Encode %v", err.Error()))
		return
	}

	s.Client.Counters.TotalBytesBeforeReduction += int64(len(byte_msg))

	if s.Client.ProducerProtoDesc != nil {
		protoMsg, err := jsonToProto(byte_msg, s.Client.ProducerProtoDesc)
		if err != nil {
			// in case of a schema mismatch, send the message as is
			s.Client.Counters.TotalBytesAfterReduction += int64(len(byte_msg))
			s.Client.Counters.TotalMessagesFailedProduce++
			return
		} else {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte("superstream_schema"),
				Value: []byte(s.Client.ProducerSchemaID),
			})
			s.Client.Counters.TotalBytesAfterReduction += int64(len(protoMsg))
			s.Client.Counters.TotalMessagesSuccessfullyProduce++
			msg.Value = sarama.ByteEncoder(protoMsg)
		}
	} else {
		s.Client.Counters.TotalBytesAfterReduction += int64(len(byte_msg))
		s.Client.Counters.TotalMessagesFailedProduce++
		if s.Client.LearningFactorCounter <= s.Client.LearningFactor {
			s.Client.SendLearningMessage(byte_msg)
			s.Client.LearningFactorCounter++
		} else if !s.Client.LearningRequestSent && s.Client.LearningFactorCounter >= s.Client.LearningFactor && s.Client.ProducerProtoDesc == nil {
			s.Client.SendRegisterSchemaReq()
		}
	}
}

func contains(slice []int32, element int32) bool {
	for _, elem := range slice {
		if elem == element {
			return true
		}
	}
	return false
}

func (s *SaramaConsumerInterceptor) OnConsume(msg *sarama.ConsumerMessage) {
	if !s.Client.IsConsumer {
		s.Client.SendClientTypeUpdateReq("consumer")
	}

	if s.Client.Config.ConsumerTopicsPartitions == nil {
		s.Client.Config.ConsumerTopicsPartitions = map[string][]int32{}
	}

	if partitions, ok := s.Client.Config.ConsumerTopicsPartitions[msg.Topic]; ok {
		if !contains(partitions, msg.Partition) {
			s.Client.Config.ConsumerTopicsPartitions[msg.Topic] = append(s.Client.Config.ConsumerTopicsPartitions[msg.Topic], msg.Partition)
		}
	} else {
		s.Client.Config.ConsumerTopicsPartitions[msg.Topic] = []int32{msg.Partition}
	}

	s.Client.Counters.TotalBytesAfterReduction += int64(len(msg.Value))

	for i, header := range msg.Headers {
		if string(header.Key) == "superstream_schema" {
			schemaID := string(header.Value)
			_, ok := s.Client.ConsumerProtoDescMap[schemaID]
			if !ok {
				if !s.Client.GetSchemaRequestSent {
					s.Client.SentGetSchemaRequest(schemaID)
				}

				for !ok {
					time.Sleep(500 * time.Millisecond)
					_, ok = s.Client.ConsumerProtoDescMap[schemaID]
				}
			}

			descriptor, ok := s.Client.ConsumerProtoDescMap[schemaID]
			if ok {
				jsonMsg, err := protoToJson(msg.Value, descriptor)
				if err != nil {
					//Print error
					s.Client.handleError(fmt.Sprintf(" OnConsume at protoToJson %v", err.Error()))
					return
				} else {
					msg.Headers = append(msg.Headers[:i], msg.Headers[i+1:]...)
					msg.Value = jsonMsg
					s.Client.Counters.TotalBytesBeforeReduction += int64(len(jsonMsg))
					s.Client.Counters.TotalMessagesSuccessfullyConsumed++
				}
			} else {
				s.Client.handleError(fmt.Sprintf(" OnConsume schema not found"))
				fmt.Println("superstream: schema not found")
				return
			}
			return
		}
	}
	s.Client.Counters.TotalBytesBeforeReduction += int64(len(msg.Value))
	s.Client.Counters.TotalMessagesFailedConsume++
}

package kafka

import "github.com/segmentio/kafka-go"

func GetPartitionCount(brokerAddr, topic string) (int, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)

	if err != nil {
		return 0, err
	}

	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)

	if err != nil {
		return 0, err
	}

	return len(partitions), nil
}

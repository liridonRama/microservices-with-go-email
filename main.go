package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/smtp"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("CONFLUENT_BOOTSTRAP_SERVER"),
		"security.protocol": os.Getenv("CONFLUENT_SECURITY_PROTOCOL"),
		"sasl.username":     os.Getenv("CONFLUENT_API_KEY"),
		"sasl.password":     os.Getenv("CONFLUENT_API_SECRET"),
		"sasl.mechanism":    os.Getenv("CONFLUENT_SASL_MECHANISM"),
		"group.id":          os.Getenv("CONFLUENT_GROUP_ID"),
		"auto.offset.reset": os.Getenv("CONFLUENT_OFFSET_RESET"),
	})

	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{"default"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		var prsdMsg map[string]interface{}

		err = json.Unmarshal(msg.Value, &prsdMsg)
		if err != nil {
			log.Panicln("Error while trying to unmarshal message:", err)
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		ambassadorMessage := []byte(fmt.Sprintf("You earned $%v from the link %v", prsdMsg["ambassadorRevenue"], prsdMsg["orderCode"]))
		smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{prsdMsg["ambassadorEmail"].(string)}, ambassadorMessage)

		adminMessage := []byte(fmt.Sprintf("Order #%v with a total of $%v has been completed", prsdMsg["orderId"], prsdMsg["adminRevenue"]))
		smtp.SendMail("host.docker.internal:1025", nil, "no-reply@email.com", []string{"admin@admin.com"}, adminMessage)
	}
}

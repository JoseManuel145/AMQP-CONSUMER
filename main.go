package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

var (
	reports []map[string]interface{}
	amqp    sync.Mutex
)

func main() {
	// Cargar variables de entorno desde el archivo .env
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error al cargar el archivo .env: %v", err)
	}

	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		log.Fatal("RABBITMQ_URL no está definida en el archivo .env")
	}

	log.Println("Iniciando rabbit")

	conn, err := amqp091.Dial(rabbitMQURL)
	failOnError(err, "no se conecto a rabbit")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "error abriendo canal de rabbit")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"report_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error en la cola")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "error en el consumidor")

	go func() {
		for d := range msgs {
			var report map[string]interface{}
			if err := json.Unmarshal(d.Body, &report); err != nil {
				log.Println("error creando el JSON:", err)
				continue
			}

			log.Printf("Mensaje recibido: %+v\n", report)

			amqp.Lock()
			reports = append(reports, report)
			amqp.Unlock()
		}
	}()

	http.HandleFunc("/reports", func(w http.ResponseWriter, r *http.Request) {
		amqp.Lock()
		defer amqp.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reports)

		reports = nil
	})

	log.Println("api del consumidor corriendo en :9090")
	log.Fatal(http.ListenAndServe(":9090", nil))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

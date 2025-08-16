// go mod init github.com/yourusername
// go get github.com/gofiber/fiber/v2
// go get github.com/segmentio/kafka-go

package main

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/segmentio/kafka-go"
)

func middleware(c *fiber.Ctx) error {
	// Do something before the request is handled
	fmt.Println("Middleware: Request received")
	fmt.Println("Method:", c.Method())
	fmt.Println("Path:", c.Path())
	fmt.Println("Body:", string(c.Body()))

	return c.Next()
}

func main() {

	app := fiber.New()
	app.Use(middleware)

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, Kafka-go!")
	})

	app.Post("/cats", createCat)
	app.Patch("/cats/:id", updateCat)
	app.Delete("/cats/:id", deleteCat)

	app.Listen(":3000")

}

func createCat(c *fiber.Ctx) error {

	// get body
	type Body struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Breed string `json:"breed"`
	}

	var body Body
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "Cannot parse JSON",
		})
	}
	// marshal struct to JSON
	jsonValue, err := json.Marshal(body)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":   true,
			"message": "Cannot marshal body to JSON",
		})
	}

	brokerAddress := "localhost:9092"
	topic := "cat_created"

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // คือ การแบ่งส่งข้อมูลไปที่ partition ที่มีข้อมูลน้อยที่สุด
		RequiredAcks: kafka.RequireAll,    // คือ การรอให้ทุก partition ทำการบันทึกข้อมูลเรียบร้อยก่อนที่จะตอบกลับ
	}

	defer w.Close() // คือ การปิด connection หลังจากทำงานเสร็จ

	err = w.WriteMessages(c.Context(), kafka.Message{
		Key:   []byte("create_cat_na"), // คือ การกำหนด partition ที่จะส่งข้อมูลไป ถ้าไม่กำหนดจะส่งไปทุก partition
		Value: jsonValue,
	})

	if err != nil {
		return c.SendString(err.Error())
	}

	return c.SendString("Message sent to Kafka!")
}

func updateCat(c *fiber.Ctx) error {

	// get body
	type Body struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Breed string `json:"breed"`
		ID    string `json:"id"`
	}

	var body Body
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "Cannot parse JSON",
		})
	}
	id := c.Params("id")

	// เพิ่ม key id ใน body
	body.ID = id

	// marshal struct to JSON
	jsonValue, err := json.Marshal(body)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":   true,
			"message": "Cannot marshal body to JSON",
		})
	}

	brokerAddress := "localhost:9092"
	topic := "cat_updated"

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // คือ การแบ่งส่งข้อมูลไปที่ partition ที่มีข้อมูลน้อยที่สุด
		RequiredAcks: kafka.RequireAll,    // คือ การรอให้ทุก partition ทำการบันทึกข้อมูลเรียบร้อยก่อนที่จะตอบกลับ
	}

	defer w.Close() // คือ การปิด connection หลังจากทำงานเสร็จ

	err = w.WriteMessages(c.Context(), kafka.Message{
		Key:   []byte("update_cat_na"), // คือ การกำหนด partition ที่จะส่งข้อมูลไป ถ้าไม่กำหนดจะส่งไปทุก partition
		Value: jsonValue,
	})

	if err != nil {
		return c.SendString(err.Error())
	}

	return c.SendString("Message sent to Kafka!")
}

func deleteCat(c *fiber.Ctx) error {

	id := c.Params("id")

	// marshal struct to JSON
	jsonValue, err := json.Marshal(fiber.Map{
		"id": id,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":   true,
			"message": "Cannot marshal body to JSON",
		})
	}

	brokerAddress := "localhost:9092"
	topic := "cat_deleted"

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // คือ การแบ่งส่งข้อมูลไปที่ partition ที่มีข้อมูลน้อยที่สุด
		RequiredAcks: kafka.RequireAll,    // คือ การรอให้ทุก partition ทำการบันทึกข้อมูลเรียบร้อยก่อนที่จะตอบกลับ
	}

	defer w.Close() // คือ การปิด connection หลังจากทำงานเสร็จ

	err = w.WriteMessages(c.Context(), kafka.Message{
		Key:   []byte("delete_cat_na"), // คือ การกำหนด partition ที่จะส่งข้อมูลไป ถ้าไม่กำหนดจะส่งไปทุก partition
		Value: jsonValue,
	})

	if err != nil {
		return c.SendString(err.Error())
	}

	return c.SendString("Message sent to Kafka!")
}

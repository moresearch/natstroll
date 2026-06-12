// spoke.go
// Run: go mod init spoke && go get github.com/nats-io/nats.go github.com/cloudevents/sdk-go/v2 && go run spoke.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/nats-io/nats.go"
)

var (
	natsURL       = os.Getenv("NATS_URL")
	userID        = os.Getenv("USER_ID")
	ollamaURL     = "http://localhost:11434/api/generate"
	smallModel    = "tinyllama"
	maxIterations = 10
)

const (
	TaskStream         = "TASK_STREAM"
	TaskSubjectPrefix  = "task.request."
	ReplySubjectPrefix = "api.reply."
	HeartbeatSubject   = "heartbeat."
	RequestType        = "com.task.request"
	ResponseType       = "com.task.response"
)

type TaskRequest struct {
	TaskID      string `json:"task_id"`
	Instruction string `json:"instruction"`
	Context     string `json:"context"`
}

type TaskResponse struct {
	TaskID string `json:"task_id"`
	Result string `json:"result"`
	Status string `json:"status"`
}

type Action struct {
	Thought string `json:"thought"`
	Action  string `json:"action"`
	Input   string `json:"input"`
}

type Heartbeat struct {
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
}

func createCloudEvent(ceType, source string, data interface{}) (*cloudevents.Event, error) {
	event := cloudevents.NewEvent()
	event.SetType(ceType)
	event.SetSource(source)
	event.SetDataContentType(cloudevents.ApplicationJSON)
	if err := event.SetData(data); err != nil {
		return nil, err
	}
	return &event, nil
}

func main() {
	if userID == "" {
		host, _ := os.Hostname()
		userID = host
	}
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	log.Printf("🔌 Connecting to NATS at %s as %s", natsURL, userID)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			hb := Heartbeat{UserID: userID, Timestamp: time.Now()}
			data, _ := json.Marshal(hb)
			subject := HeartbeatSubject + userID
			if err := nc.Publish(subject, data); err != nil {
				log.Printf("⚠️ Heartbeat publish error: %v", err)
			} else {
				log.Printf("💓 Heartbeat sent to %s", subject)
			}
		}
	}()

	filterSubject := TaskSubjectPrefix + userID
	consumerName := fmt.Sprintf("spoke_%s", userID)
	log.Printf("📡 Creating pull consumer on %s", filterSubject)

	_, err = js.AddConsumer(TaskStream, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: filterSubject,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := js.PullSubscribe(filterSubject, consumerName, nats.BindStream(TaskStream))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("✅ Spoke %s listening for tasks on %s", userID, filterSubject)

	for {
		msgs, err := consumer.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			if err != nats.ErrTimeout {
				log.Printf("Fetch error: %v", err)
			}
			continue
		}
		for _, msg := range msgs {
			log.Printf("📬 Received message on %s", msg.Subject)
			var event cloudevents.Event
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				log.Printf("❌ Failed to unmarshal CloudEvent: %v", err)
				msg.Nak()
				continue
			}
			var taskReq TaskRequest
			if err := event.DataAs(&taskReq); err != nil {
				log.Printf("❌ Failed to extract task request: %v", err)
				msg.Nak()
				continue
			}
			log.Printf("🤖 Task %s: %s", taskReq.TaskID, taskReq.Instruction)

			result, err := runReActLoop(taskReq.Instruction)
			status := "success"
			if err != nil {
				result = err.Error()
				status = "failure"
				log.Printf("❌ Task %s failed: %v", taskReq.TaskID, err)
			} else {
				log.Printf("✅ Task %s completed successfully", taskReq.TaskID)
			}

			respData := TaskResponse{
				TaskID: taskReq.TaskID,
				Result: result,
				Status: status,
			}
			respEvent, _ := createCloudEvent(ResponseType, "spoke", respData)
			respBytes, _ := json.Marshal(respEvent)
			if err := msg.Respond(respBytes); err != nil {
				log.Printf("⚠️ Failed to respond: %v", err)
			}
			msg.Ack()
			log.Printf("📤 Response sent for task %s", taskReq.TaskID)
		}
	}
}

func runReActLoop(instruction string) (string, error) {
	systemPrompt := `You are an automation agent. You have these skills:
1. read_file: read a text file. Input: {"path": "/path/to/file"}
2. write_file: write content to a file. Input: {"path": "/path/to/file", "content": "text"}
3. run_command: execute a shell command. Input: {"command": "ls -la"}

Respond ONLY with JSON: {"thought": "...", "action": "skill_name", "input": {...}}
When finished, set action to "finish" and input to {"result": "final output"}.`
	conversation := []map[string]string{
		{"role": "system", "content": systemPrompt},
		{"role": "user", "content": instruction},
	}

	for i := 0; i < maxIterations; i++ {
		log.Printf("🧠 ReAct step %d calling Ollama...", i+1)
		response, err := callOllama(conversation)
		if err != nil {
			return "", fmt.Errorf("Ollama error: %v", err)
		}
		log.Printf("📝 LLM response: %s", response)

		var action Action
		if err := json.Unmarshal([]byte(response), &action); err != nil {
			return "", fmt.Errorf("invalid JSON from LLM: %s", response)
		}
		log.Printf("💭 Thought: %s", action.Thought)

		if action.Action == "finish" {
			var finishRes struct{ Result string }
			json.Unmarshal([]byte(action.Input), &finishRes)
			log.Printf("🏁 Finished: %s", finishRes.Result)
			return finishRes.Result, nil
		}

		log.Printf("🔧 Executing skill %s with input %s", action.Action, action.Input)
		observation, err := executeSkill(action.Action, action.Input)
		if err != nil {
			observation = fmt.Sprintf("Error: %v", err)
			log.Printf("⚠️ Skill error: %v", err)
		} else {
			log.Printf("👀 Observation: %s", observation)
		}

		conversation = append(conversation, map[string]string{"role": "assistant", "content": response})
		conversation = append(conversation, map[string]string{"role": "user", "content": observation})
	}
	return "", fmt.Errorf("max iterations reached")
}

func callOllama(messages []map[string]string) (string, error) {
	var prompt strings.Builder
	for _, m := range messages {
		prompt.WriteString(m["role"] + ": " + m["content"] + "\n")
	}
	reqBody := map[string]interface{}{
		"model":  smallModel,
		"prompt": prompt.String(),
		"stream": false,
		"format": "json",
	}
	jsonData, _ := json.Marshal(reqBody)
	resp, err := http.Post(ollamaURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result struct{ Response string }
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	return result.Response, nil
}

func executeSkill(action, inputStr string) (string, error) {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputStr), &input); err != nil {
		return "", fmt.Errorf("invalid input JSON: %v", err)
	}
	switch action {
	case "read_file":
		path, ok := input["path"].(string)
		if !ok {
			return "", fmt.Errorf("missing 'path'")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return string(data), nil
	case "write_file":
		path, ok := input["path"].(string)
		if !ok {
			return "", fmt.Errorf("missing 'path'")
		}
		content, ok := input["content"].(string)
		if !ok {
			return "", fmt.Errorf("missing 'content'")
		}
		err := os.WriteFile(path, []byte(content), 0644)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("wrote to %s", path), nil
	case "run_command":
		cmdStr, ok := input["command"].(string)
		if !ok {
			return "", fmt.Errorf("missing 'command'")
		}
		out, err := exec.Command("sh", "-c", cmdStr).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("%s: %v", string(out), err)
		}
		return string(out), nil
	default:
		return "", fmt.Errorf("unknown skill: %s", action)
	}
}

package shared_test

import (
	"encoding/json"
	"fmt"

	"github.com/moresearch/natstroll/internal/shared"
)

func ExampleJokeRequest_roundTrip() {
	req := shared.JokeRequest{
		RequestID:   "abc-123",
		Joke:        "Why did the gopher cross the road?",
		FromSpokeID: "hub",
	}
	data, _ := json.Marshal(req)

	var decoded shared.JokeRequest
	_ = json.Unmarshal(data, &decoded)

	fmt.Println(decoded.RequestID)
	fmt.Println(decoded.Joke)
	// Output:
	// abc-123
	// Why did the gopher cross the road?
}

func ExampleJokeResponse_roundTrip() {
	resp := shared.JokeResponse{
		RequestID:     "abc-123",
		Reply:         "To get to the other NATS cluster.",
		ReplyingSpoke: "black-spoke",
	}
	data, _ := json.Marshal(resp)

	var decoded shared.JokeResponse
	_ = json.Unmarshal(data, &decoded)

	fmt.Println(decoded.RequestID)
	fmt.Println(decoded.Reply)
	fmt.Println(decoded.ReplyingSpoke)
	// Output:
	// abc-123
	// To get to the other NATS cluster.
	// black-spoke
}

func ExampleRegisterResponse_roundTrip() {
	resp := shared.RegisterResponse{CredsData: "jwt-credentials-payload"}
	data, _ := json.Marshal(resp)

	var decoded shared.RegisterResponse
	_ = json.Unmarshal(data, &decoded)

	fmt.Println(decoded.CredsData)
	// Output:
	// jwt-credentials-payload
}

func ExampleValidateSpokeID() {
	fmt.Println(shared.ValidateSpokeID("my-spoke_1"))
	fmt.Println(shared.ValidateSpokeID(""))
	fmt.Println(shared.ValidateSpokeID("bad id"))
	// Output:
	// <nil>
	// spoke id is empty
	// spoke id "bad id" contains invalid character ' '
}

func ExampleSafeName() {
	fmt.Println(shared.SafeName("my.spoke-1"))
	fmt.Println(shared.SafeName("a:b/c\\d"))
	// Output:
	// my_spoke_1
	// a_b_c_d
}

func ExampleConsumerNameForSpoke() {
	fmt.Println(shared.ConsumerNameForSpoke("black-spoke"))
	// Output:
	// joke_consumer_black_spoke
}

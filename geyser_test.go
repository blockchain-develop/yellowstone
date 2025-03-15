package yellowstone_geyser

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	yellowstone_geyser_pb "github.com/blockchain-develop/yellowstone/pb"
)

func TestSubscribeTransaction(t *testing.T) {
	client, err := Connect(context.Background(), "https://solana-yellowstone-grpc.publicnode.com:443", "")
	if err != nil {
		panic(err)
	}
	vote := false
	failed := false
	req := &yellowstone_geyser_pb.SubscribeRequestFilterTransactions{
		Vote:   &vote,
		Failed: &failed,
	}
	sub, err := client.SubscribeTransaction(req)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := sub.Recv()
		if err != nil {
			fmt.Printf("recv error: %v\n", err)
			return
		}
		msgJson, _ := json.MarshalIndent(msg, "", "    ")
		os.WriteFile("subscribe_update.json", msgJson, 0644)
	}
}

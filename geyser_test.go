package yellowstone_geyser

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	yellowstone_geyser_pb "github.com/blockchain-develop/yellowstone/pb"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var mockCu uint64 = 200000

// todo add more test coverage
func TestConvertTransaction(t *testing.T) {
	tests := []struct {
		name        string
		input       *yellowstone_geyser_pb.SubscribeUpdateTransaction
		expectError bool
		verify      func(*testing.T, *rpc.GetTransactionResult)
	}{
		{
			name: "Basic Transaction Conversion",
			input: &yellowstone_geyser_pb.SubscribeUpdateTransaction{
				Transaction: &yellowstone_geyser_pb.SubscribeUpdateTransactionInfo{
					Transaction: &yellowstone_geyser_pb.Transaction{
						Message: &yellowstone_geyser_pb.Message{
							Header: &yellowstone_geyser_pb.MessageHeader{
								NumRequiredSignatures:       1,
								NumReadonlySignedAccounts:   0,
								NumReadonlyUnsignedAccounts: 1,
							},
							RecentBlockhash: make([]byte, 32),
							Instructions: []*yellowstone_geyser_pb.CompiledInstruction{
								{
									ProgramIdIndex: 0,
									Accounts:       []byte{0, 1},
									Data:           []byte{1, 2, 3},
								},
							},
							Versioned: false,
						},
						Signatures: [][]byte{make([]byte, 64)},
					},
					Meta: &yellowstone_geyser_pb.TransactionStatusMeta{
						Fee:                  1000,
						ComputeUnitsConsumed: &mockCu,
						PreBalances:          []uint64{100, 200},
						PostBalances:         []uint64{90, 210},
						LogMessages:          []string{"log1", "log2"},
						PreTokenBalances: []*yellowstone_geyser_pb.TokenBalance{
							{
								AccountIndex: 1,
								Mint:         "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
								Owner:        "11111111111111111111111111111111",
								UiTokenAmount: &yellowstone_geyser_pb.UiTokenAmount{
									Amount:         "1000000",
									Decimals:       6,
									UiAmount:       1.0,
									UiAmountString: "1.0",
								},
							},
						},
					},
				},
			},
			expectError: false,
			verify: func(t *testing.T, result *rpc.GetTransactionResult) {
				assert.NotNil(t, result)
				assert.Equal(t, uint64(1000), result.Meta.Fee)
				assert.Equal(t, &mockCu, result.Meta.ComputeUnitsConsumed)
				assert.Equal(t, []uint64{100, 200}, result.Meta.PreBalances)
				assert.Equal(t, []uint64{90, 210}, result.Meta.PostBalances)
				assert.Equal(t, []string{"log1", "log2"}, result.Meta.LogMessages)

				// Verify PreTokenBalances
				require.Len(t, result.Meta.PreTokenBalances, 1)
				assert.Equal(t, uint16(1), result.Meta.PreTokenBalances[0].AccountIndex)
				assert.Equal(t, "1.0", result.Meta.PreTokenBalances[0].UiTokenAmount.UiAmountString)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertTransaction(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.verify != nil {
				tt.verify(t, result)
			}
		})
	}
}

func TestSubscribe(t *testing.T) {
	client, err := New(context.Background(), "https://solana-yellowstone-grpc.publicnode.com:443", nil)
	if err != nil {
		panic(err)
	}
	client.AddStreamClient(context.Background(), "test", yellowstone_geyser_pb.CommitmentLevel_PROCESSED)
	stream := client.GetStreamClient("test")
	vote := false
	failed := false
	stream.SubscribeTransaction("tet", &yellowstone_geyser_pb.SubscribeRequestFilterTransactions{
		Vote:   &vote,
		Failed: &failed,
	})
	for {
		select {
		case msg := <-stream.Ch:
			msgJson, _ := json.Marshal(msg)
			fmt.Printf("msg: %s\n", string(msgJson))
			transaction := msg.GetTransaction()
			if transaction == nil {
				continue
			}
			fmt.Printf("get transaction: %d\n", transaction.Slot)
		}
	}

}

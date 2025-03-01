package yellowstone_geyser

import (
	"reflect"
	"strconv"
	"unsafe"

	yellowstone_geyser_pb "github.com/blockchain-develop/yellowstone/pb"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// ConvertTransaction converts a Geyser parsed transaction into *rpc.GetTransactionResult.
func ConvertTransaction(slot uint64, tt uint64, geyserTx *yellowstone_geyser_pb.SubscribeUpdateTransactionInfo) (*rpc.GetTransactionResult, error) {
	meta := geyserTx.Meta
	transaction := geyserTx.Transaction
	blockTime := solana.UnixTimeSeconds(int64(tt))
	tx := &rpc.GetTransactionResult{
		Transaction: &rpc.TransactionResultEnvelope{},
		Meta: &rpc.TransactionMeta{
			InnerInstructions: make([]rpc.InnerInstruction, 0),
			LogMessages:       make([]string, 0),
			PostBalances:      make([]uint64, 0),
			PostTokenBalances: make([]rpc.TokenBalance, 0),
			PreBalances:       make([]uint64, 0),
			PreTokenBalances:  make([]rpc.TokenBalance, 0),
			Rewards:           make([]rpc.BlockReward, 0),
			LoadedAddresses: rpc.LoadedAddresses{
				ReadOnly: make([]solana.PublicKey, 0),
				Writable: make([]solana.PublicKey, 0),
			},
		},
		Slot:      slot,
		BlockTime: &blockTime,
	}

	tx.Meta.PreBalances = meta.PreBalances
	tx.Meta.PostBalances = meta.PostBalances
	tx.Meta.Err = meta.Err
	tx.Meta.Fee = meta.Fee
	tx.Meta.ComputeUnitsConsumed = meta.ComputeUnitsConsumed
	tx.Meta.LogMessages = meta.LogMessages

	for _, preTokenBalance := range meta.PreTokenBalances {
		owner := solana.MustPublicKeyFromBase58(preTokenBalance.Owner)
		tx.Meta.PreTokenBalances = append(tx.Meta.PreTokenBalances, rpc.TokenBalance{
			AccountIndex: uint16(preTokenBalance.AccountIndex),
			Owner:        &owner,
			Mint:         solana.MustPublicKeyFromBase58(preTokenBalance.Mint),
			UiTokenAmount: &rpc.UiTokenAmount{
				Amount:         preTokenBalance.UiTokenAmount.Amount,
				Decimals:       uint8(preTokenBalance.UiTokenAmount.Decimals),
				UiAmount:       &preTokenBalance.UiTokenAmount.UiAmount,
				UiAmountString: preTokenBalance.UiTokenAmount.UiAmountString,
			},
		})
	}

	for _, postTokenBalance := range meta.PostTokenBalances {
		owner := solana.MustPublicKeyFromBase58(postTokenBalance.Owner)
		tx.Meta.PostTokenBalances = append(tx.Meta.PostTokenBalances, rpc.TokenBalance{
			AccountIndex: uint16(postTokenBalance.AccountIndex),
			Owner:        &owner,
			Mint:         solana.MustPublicKeyFromBase58(postTokenBalance.Mint),
			UiTokenAmount: &rpc.UiTokenAmount{
				Amount:         postTokenBalance.UiTokenAmount.Amount,
				Decimals:       uint8(postTokenBalance.UiTokenAmount.Decimals),
				UiAmount:       &postTokenBalance.UiTokenAmount.UiAmount,
				UiAmountString: postTokenBalance.UiTokenAmount.UiAmountString,
			},
		})
	}

	for i, innerInst := range meta.InnerInstructions {
		tx.Meta.InnerInstructions = append(tx.Meta.InnerInstructions, rpc.InnerInstruction{})
		tx.Meta.InnerInstructions[i].Index = uint16(innerInst.Index)
		for x, inst := range innerInst.Instructions {
			tx.Meta.InnerInstructions[i].Instructions = append(tx.Meta.InnerInstructions[i].Instructions, solana.CompiledInstruction{})
			accounts, err := bytesToUint16Slice(inst.Accounts)
			if err != nil {
				return nil, err
			}

			tx.Meta.InnerInstructions[i].Instructions[x].Accounts = accounts
			tx.Meta.InnerInstructions[i].Instructions[x].ProgramIDIndex = uint16(inst.ProgramIdIndex)
			tx.Meta.InnerInstructions[i].Instructions[x].Data = inst.Data
			tx.Meta.InnerInstructions[i].Instructions[x].StackHeight = uint16(*inst.StackHeight)
			// if err = tx.Meta.InnerInstructions[i].Instructions[x].Data.UnmarshalJSON(inst.Data); err != nil {
			// 	return nil, err
			// }
		}
	}

	for _, reward := range meta.Rewards {
		comm, _ := strconv.ParseUint(reward.Commission, 10, 64)
		commission := uint8(comm)
		tx.Meta.Rewards = append(tx.Meta.Rewards, rpc.BlockReward{
			Pubkey:      solana.MustPublicKeyFromBase58(reward.Pubkey),
			Lamports:    reward.Lamports,
			PostBalance: reward.PostBalance,
			RewardType:  rpc.RewardType(reward.RewardType.String()),
			Commission:  &commission,
		})
	}

	for _, readOnlyAddress := range meta.LoadedReadonlyAddresses {
		tx.Meta.LoadedAddresses.ReadOnly = append(tx.Meta.LoadedAddresses.ReadOnly, solana.PublicKeyFromBytes(readOnlyAddress))
	}

	for _, writableAddress := range meta.LoadedWritableAddresses {
		tx.Meta.LoadedAddresses.ReadOnly = append(tx.Meta.LoadedAddresses.ReadOnly, solana.PublicKeyFromBytes(writableAddress))
	}

	// solTx, err := tx.Transaction.GetTransaction()
	// if err != nil {
	// 	return nil, err
	// }

	solTx := &solana.Transaction{
		Signatures: make([]solana.Signature, 0),
		Message: solana.Message{
			AccountKeys:         make(solana.PublicKeySlice, 0),
			Instructions:        make([]solana.CompiledInstruction, 0),
			AddressTableLookups: make(solana.MessageAddressTableLookupSlice, 0),
		},
	}

	if transaction.Message.Versioned {
		solTx.Message.SetVersion(1)
	}

	solTx.Message.RecentBlockhash = solana.HashFromBytes(transaction.Message.RecentBlockhash)
	solTx.Message.Header = solana.MessageHeader{
		NumRequiredSignatures:       uint8(transaction.Message.Header.NumRequiredSignatures),
		NumReadonlySignedAccounts:   uint8(transaction.Message.Header.NumReadonlySignedAccounts),
		NumReadonlyUnsignedAccounts: uint8(transaction.Message.Header.NumReadonlyUnsignedAccounts),
	}

	for _, sig := range transaction.Signatures {
		solTx.Signatures = append(solTx.Signatures, solana.SignatureFromBytes(sig))
	}

	for _, table := range transaction.Message.AddressTableLookups {
		solTx.Message.AddressTableLookups = append(solTx.Message.AddressTableLookups, solana.MessageAddressTableLookup{
			AccountKey:      solana.PublicKeyFromBytes(table.AccountKey),
			WritableIndexes: table.WritableIndexes,
			ReadonlyIndexes: table.ReadonlyIndexes,
		})
	}

	for _, key := range transaction.Message.AccountKeys {
		solTx.Message.AccountKeys = append(solTx.Message.AccountKeys, solana.PublicKeyFromBytes(key))
	}

	for _, inst := range transaction.Message.Instructions {
		accounts, err := bytesToUint16Slice(inst.Accounts)
		if err != nil {
			return nil, err
		}

		solTx.Message.Instructions = append(solTx.Message.Instructions, solana.CompiledInstruction{
			ProgramIDIndex: uint16(inst.ProgramIdIndex),
			Accounts:       accounts,
			Data:           inst.Data,
		})
	}

	func(obj interface{}, fieldName string, value interface{}) {
		v := reflect.ValueOf(obj).Elem()
		f := v.FieldByName(fieldName)
		ptr := unsafe.Pointer(f.UnsafeAddr())
		reflect.NewAt(f.Type(), ptr).Elem().Set(reflect.ValueOf(value))
	}(tx.Transaction, "asParsedTransaction", solTx)

	return tx, nil
}

// ConvertBlockHash converts a Geyser type block to a github.com/gagliardetto/solana-go Solana block.
func ConvertBlock(geyserBlock *yellowstone_geyser_pb.SubscribeUpdateBlock) (*rpc.GetBlockResult, error) {
	block := &rpc.GetBlockResult{}

	blockTime := solana.UnixTimeSeconds(geyserBlock.BlockTime.Timestamp)
	block.BlockTime = &blockTime
	block.BlockHeight = &geyserBlock.BlockHeight.BlockHeight
	block.Blockhash = solana.Hash{[]byte(geyserBlock.Blockhash)[32]}
	block.ParentSlot = geyserBlock.ParentSlot

	for _, reward := range geyserBlock.Rewards.Rewards {
		commission, err := strconv.ParseUint(reward.Commission, 10, 8)
		if err != nil {
			return nil, nil
		}

		var rewardType rpc.RewardType
		switch reward.RewardType {
		case 1:
			rewardType = rpc.RewardTypeFee
		case 2:
			rewardType = rpc.RewardTypeRent
		case 3:
			rewardType = rpc.RewardTypeStaking
		case 4:
			rewardType = rpc.RewardTypeVoting
		}

		comm := uint8(commission)
		block.Rewards = append(block.Rewards, rpc.BlockReward{
			Pubkey:      solana.MustPublicKeyFromBase58(reward.Pubkey),
			Lamports:    reward.Lamports,
			PostBalance: reward.PostBalance,
			Commission:  &comm,
			RewardType:  rewardType,
		})
	}
	return block, nil
}

func bytesToUint16Slice(data []byte) ([]uint16, error) {
	uint16s := make([]uint16, len(data))

	for i := 0; i < len(data); i += 1 {
		uint16s[i] = uint16(data[i])
	}

	return uint16s, nil
}

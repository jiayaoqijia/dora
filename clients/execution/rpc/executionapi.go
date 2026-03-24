package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/url"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"

	"github.com/ethpandaops/dora/clients/sshtunnel"
)

type ExecutionClient struct {
	name      string
	endpoint  string
	headers   map[string]string
	sshtunnel *sshtunnel.SSHTunnel
	rpcClient *rpc.Client
	ethClient *ethclient.Client
}

// NewExecutionClient is used to create a new execution client
func NewExecutionClient(name, endpoint string, headers map[string]string, sshcfg *sshtunnel.SshConfig, logger logrus.FieldLogger) (*ExecutionClient, error) {
	client := &ExecutionClient{
		name:     name,
		endpoint: endpoint,
		headers:  headers,
	}

	if sshcfg != nil {
		// create ssh tunnel to remote host
		sshPort := 0
		if sshcfg.Port != "" {
			sshPort, _ = strconv.Atoi(sshcfg.Port)
		}
		if sshPort == 0 {
			sshPort = 22
		}
		sshEndpoint := fmt.Sprintf("%v@%v:%v", sshcfg.User, sshcfg.Host, sshPort)
		var sshAuth ssh.AuthMethod
		if sshcfg.Keyfile != "" {
			var err error
			sshAuth, err = sshtunnel.PrivateKeyFile(sshcfg.Keyfile)
			if err != nil {
				return nil, fmt.Errorf("could not load ssh keyfile: %w", err)
			}
		} else {
			sshAuth = ssh.Password(sshcfg.Password)
		}

		// get tunnel target from endpoint url
		endpointUrl, _ := url.Parse(endpoint)
		tunTarget := endpointUrl.Host
		if endpointUrl.Port() != "" {
			tunTarget = fmt.Sprintf("%v:%v", tunTarget, endpointUrl.Port())
		} else {
			tunTargetPort := 80
			if endpointUrl.Scheme == "https:" {
				tunTargetPort = 443
			}
			tunTarget = fmt.Sprintf("%v:%v", tunTarget, tunTargetPort)
		}

		client.sshtunnel = sshtunnel.NewSSHTunnel(sshEndpoint, sshAuth, tunTarget)
		client.sshtunnel.Log = logger.WithField("sshtun", sshcfg.Host)
		err := client.sshtunnel.Start()
		if err != nil {
			return nil, fmt.Errorf("could not start ssh tunnel: %w", err)
		}

		// override endpoint to use local tunnel end
		endpointUrl.Host = fmt.Sprintf("localhost:%v", client.sshtunnel.Local.Port)

		client.endpoint = endpointUrl.String()
	}

	return client, nil
}

func (ec *ExecutionClient) Initialize(ctx context.Context) error {
	if ec.ethClient != nil {
		return nil
	}

	rpcClient, err := rpc.DialContext(ctx, ec.endpoint)
	if err != nil {
		return err
	}

	for hKey, hVal := range ec.headers {
		rpcClient.SetHeader(hKey, hVal)
	}

	ec.rpcClient = rpcClient
	ec.ethClient = ethclient.NewClient(rpcClient)

	return nil
}

func (ec *ExecutionClient) GetEthClient() *ethclient.Client {
	return ec.ethClient
}

func (ec *ExecutionClient) GetClientVersion(ctx context.Context) (string, error) {
	var result string
	err := ec.rpcClient.CallContext(ctx, &result, "web3_clientVersion")

	return result, err
}

func (ec *ExecutionClient) GetChainSpec(ctx context.Context) (*ChainSpec, error) {
	chainID, err := ec.ethClient.ChainID(ctx)
	if err != nil {
		return nil, err
	}

	return &ChainSpec{
		ChainID: chainID.String(),
	}, nil
}

func (ec *ExecutionClient) GetAdminPeers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	var result []*p2p.PeerInfo
	err := ec.rpcClient.CallContext(ctx, &result, "admin_peers")
	// Workaround for Nethermind that expects an additional boolean
	if err != nil && err.Error() == "Invalid params" {
		result = nil
		err = ec.rpcClient.CallContext(ctx, &result, "admin_peers", false)
	}
	return result, err
}

func (ec *ExecutionClient) GetAdminNodeInfo(ctx context.Context) (*p2p.NodeInfo, error) {
	var result *p2p.NodeInfo
	err := ec.rpcClient.CallContext(ctx, &result, "admin_nodeInfo")
	return result, err
}

func (ec *ExecutionClient) GetEthConfig(ctx context.Context) (*EthConfig, error) {
	var result *EthConfig
	err := ec.rpcClient.CallContext(ctx, &result, "eth_config")
	return result, err
}

func (ec *ExecutionClient) GetNodeSyncing(ctx context.Context) (*SyncStatus, error) {
	status, err := ec.ethClient.SyncProgress(ctx)
	if err != nil {
		return nil, err
	}

	if status == nil {
		// Not syncing
		ss := &SyncStatus{}
		ss.IsSyncing = false

		return ss, nil
	}

	return &SyncStatus{
		IsSyncing:     true,
		CurrentBlock:  status.CurrentBlock,
		HighestBlock:  status.HighestBlock,
		StartingBlock: status.StartingBlock,
	}, nil
}

type BlockFilterId string

func (ec *ExecutionClient) NewBlockFilter(ctx context.Context) (BlockFilterId, error) {
	var result BlockFilterId
	err := ec.rpcClient.CallContext(ctx, &result, "eth_newBlockFilter")
	return result, err
}

func (ec *ExecutionClient) GetFilterChanges(ctx context.Context, filterId BlockFilterId) ([]string, error) {
	var result []string
	err := ec.rpcClient.CallContext(ctx, &result, "eth_getFilterChanges", filterId)
	return result, err
}

func (ec *ExecutionClient) UninstallBlockFilter(ctx context.Context, filterId BlockFilterId) (bool, error) {
	var result bool
	err := ec.rpcClient.CallContext(ctx, &result, "eth_uninstallFilter", filterId)
	return result, err
}

func (ec *ExecutionClient) GetLatestHeader(ctx context.Context) (*types.Header, error) {
	header, err := ec.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (ec *ExecutionClient) GetLatestBlock(ctx context.Context) (*types.Block, error) {
	block, err := ec.ethClient.BlockByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (ec *ExecutionClient) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	header, err := ec.ethClient.HeaderByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (ec *ExecutionClient) GetHeaderByNumber(ctx context.Context, number uint64) (*types.Header, error) {
	block, err := ec.ethClient.HeaderByNumber(ctx, big.NewInt(0).SetUint64(number))
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (ec *ExecutionClient) GetBlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	block, err := ec.ethClient.BlockByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (ec *ExecutionClient) GetNonceAt(ctx context.Context, wallet common.Address, blockNumber *big.Int) (uint64, error) {
	return ec.ethClient.NonceAt(ctx, wallet, blockNumber)
}

func (ec *ExecutionClient) GetBalanceAt(ctx context.Context, wallet common.Address, blockNumber *big.Int) (*big.Int, error) {
	return ec.ethClient.BalanceAt(ctx, wallet, blockNumber)
}

func (ec *ExecutionClient) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	return ec.ethClient.TransactionReceipt(ctx, txHash)
}

func (ec *ExecutionClient) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return ec.ethClient.SendTransaction(ctx, tx)
}

// BlockInfo contains basic block information without full transaction data
type BlockInfo struct {
	Number        *big.Int
	Hash          common.Hash
	ParentHash    common.Hash
	StateRoot     common.Hash
	ReceiptsRoot  common.Hash
	GasUsed       uint64
	GasLimit      uint64
	Coinbase      common.Address
	Timestamp     uint64
	BaseFeePerGas *big.Int
	ExtraData     []byte
	PrevRandao    common.Hash
	LogsBloom     []byte
	Transactions  int
}

// GetBlockInfoByHash gets block info by hash using raw JSON-RPC to avoid transaction parsing issues
// with unsupported transaction types (e.g., EIP-7702)
func (ec *ExecutionClient) GetBlockInfoByHash(ctx context.Context, hash common.Hash) (*BlockInfo, error) {
	var result struct {
		Number        string   `json:"number"`
		Hash          string   `json:"hash"`
		ParentHash    string   `json:"parentHash"`
		StateRoot     string   `json:"stateRoot"`
		ReceiptsRoot  string   `json:"receiptsRoot"`
		GasUsed       string   `json:"gasUsed"`
		GasLimit      string   `json:"gasLimit"`
		Miner         string   `json:"miner"`
		Timestamp     string   `json:"timestamp"`
		BaseFeePerGas string   `json:"baseFeePerGas"`
		ExtraData     string   `json:"extraData"`
		Difficulty    string   `json:"difficulty"`
		LogsBloom     string   `json:"logsBloom"`
		Transactions  []string `json:"transactions"`
	}

	err := ec.rpcClient.CallContext(ctx, &result, "eth_getBlockByHash", hash.Hex(), false)
	if err != nil {
		return nil, err
	}

	if result.Number == "" {
		return nil, fmt.Errorf("block not found")
	}

	number, ok := new(big.Int).SetString(result.Number[2:], 16)
	if !ok {
		return nil, fmt.Errorf("invalid block number")
	}

	gasUsed, _ := new(big.Int).SetString(result.GasUsed[2:], 16)
	gasLimit, _ := new(big.Int).SetString(result.GasLimit[2:], 16)
	timestamp, _ := new(big.Int).SetString(result.Timestamp[2:], 16)
	var baseFeePerGas *big.Int
	if result.BaseFeePerGas != "" {
		baseFeePerGas, _ = new(big.Int).SetString(result.BaseFeePerGas[2:], 16)
	}
	var extraData []byte
	if result.ExtraData != "" {
		extraData, _ = hex.DecodeString(result.ExtraData[2:])
	}
	var prevRandao common.Hash
	if result.Difficulty != "" {
		diffBytes, _ := hex.DecodeString(result.Difficulty[2:])
		copy(prevRandao[:], diffBytes)
	}
	var logsBloom []byte
	if result.LogsBloom != "" {
		logsBloom, _ = hex.DecodeString(result.LogsBloom[2:])
	}

	return &BlockInfo{
		Number:        number,
		Hash:          common.HexToHash(result.Hash),
		ParentHash:    common.HexToHash(result.ParentHash),
		StateRoot:     common.HexToHash(result.StateRoot),
		ReceiptsRoot:  common.HexToHash(result.ReceiptsRoot),
		GasUsed:       gasUsed.Uint64(),
		GasLimit:      gasLimit.Uint64(),
		Coinbase:      common.HexToAddress(result.Miner),
		Timestamp:     timestamp.Uint64(),
		BaseFeePerGas: baseFeePerGas,
		ExtraData:     extraData,
		PrevRandao:    prevRandao,
		LogsBloom:     logsBloom,
		Transactions:  len(result.Transactions),
	}, nil
}

// GetBlockTransactionsByHash fetches transaction details from an EL block.
// Returns a slice of TransactionDetail for rendering.
// This is used for Gloas/Heze blocks where transactions are not available in the beacon block.
func (ec *ExecutionClient) GetBlockTransactionsByHash(ctx context.Context, hash common.Hash) ([]TransactionDetail, error) {
	// Use raw JSON-RPC to get block with full transaction objects
	var result struct {
		Transactions []jsonTxDetail `json:"transactions"`
	}
	err := ec.rpcClient.CallContext(ctx, &result, "eth_getBlockByHash", hash.Hex(), true)
	if err != nil {
		return nil, fmt.Errorf("eth_getBlockByHash failed: %w", err)
	}

	transactions := make([]TransactionDetail, 0, len(result.Transactions))
	for i, tx := range result.Transactions {
		detail := TransactionDetail{
			Index: uint64(i),
		}
		
		// Parse hash
		if len(tx.Hash) > 2 {
			hashBytes, _ := hex.DecodeString(tx.Hash[2:])
			if len(hashBytes) == 32 {
				detail.Hash = common.BytesToHash(hashBytes)
			}
		}
		
		// Parse from
		if len(tx.From) > 2 {
			fromBytes, _ := hex.DecodeString(tx.From[2:])
			detail.From = common.BytesToAddress(fromBytes)
		}
		
		// Parse to
		if tx.To != nil && len(*tx.To) > 2 {
			toBytes, _ := hex.DecodeString((*tx.To)[2:])
			toAddr := common.BytesToAddress(toBytes)
			detail.To = &toAddr
		}
		
		// Parse value
		if len(tx.Value) > 2 {
			detail.Value = new(big.Int)
			detail.Value.SetString(tx.Value[2:], 16)
		}
		
		// Parse gas
		if len(tx.Gas) > 2 {
			gasVal, _ := new(big.Int).SetString(tx.Gas[2:], 16)
			if gasVal != nil {
				detail.Gas = gasVal.Uint64()
			}
		}
		
		// Parse input
		if len(tx.Input) > 2 {
			detail.Input, _ = hex.DecodeString(tx.Input[2:])
		}
		
		// Parse type
		if len(tx.Type) > 2 {
			typeVal, _ := new(big.Int).SetString(tx.Type[2:], 16)
			if typeVal != nil {
				detail.Type = typeVal.Uint64()
			}
		}
		
		// Parse chainId
		if len(tx.ChainID) > 2 {
			detail.ChainID = new(big.Int)
			detail.ChainID.SetString(tx.ChainID[2:], 16)
		}
		
		// Parse nonce
		if len(tx.Nonce) > 2 {
			nonceVal, _ := new(big.Int).SetString(tx.Nonce[2:], 16)
			if nonceVal != nil {
				detail.Nonce = nonceVal.Uint64()
			}
		}
		
		// Parse gasPrice
		if len(tx.GasPrice) > 2 {
			detail.GasPrice = new(big.Int)
			detail.GasPrice.SetString(tx.GasPrice[2:], 16)
		}
		
		transactions = append(transactions, detail)
	}

	return transactions, nil
}

// TransactionDetail represents transaction details from EL JSON-RPC response
type TransactionDetail struct {
	Index    uint64
	Hash     common.Hash
	From     common.Address
	To       *common.Address
	Value    *big.Int
	Gas      uint64
	Input    []byte
	Type     uint64
	ChainID  *big.Int
	Nonce    uint64
	GasPrice *big.Int
}

// GetTransactionByHash fetches a single transaction by hash using raw JSON-RPC.
// This supports all transaction types including Type 5 (native AA) which go-ethereum doesn't support.
func (ec *ExecutionClient) GetTransactionByHash(ctx context.Context, hash common.Hash) (*TransactionDetail, error) {
	var tx jsonTxDetail
	err := ec.rpcClient.CallContext(ctx, &tx, "eth_getTransactionByHash", hash.Hex())
	if err != nil {
		return nil, fmt.Errorf("eth_getTransactionByHash failed: %w", err)
	}
	if tx.Hash == "" {
		return nil, fmt.Errorf("transaction not found")
	}

	detail := &TransactionDetail{}

	// Parse hash
	if len(tx.Hash) > 2 {
		hashBytes, _ := hex.DecodeString(tx.Hash[2:])
		if len(hashBytes) == 32 {
			detail.Hash = common.BytesToHash(hashBytes)
		}
	}

	// Parse from
	if len(tx.From) > 2 {
		fromBytes, _ := hex.DecodeString(tx.From[2:])
		detail.From = common.BytesToAddress(fromBytes)
	}

	// Parse to
	if tx.To != nil && len(*tx.To) > 2 {
		toBytes, _ := hex.DecodeString((*tx.To)[2:])
		toAddr := common.BytesToAddress(toBytes)
		detail.To = &toAddr
	}

	// Parse value
	if len(tx.Value) > 2 {
		detail.Value = new(big.Int)
		detail.Value.SetString(tx.Value[2:], 16)
	}

	// Parse gas
	if len(tx.Gas) > 2 {
		gasVal, _ := new(big.Int).SetString(tx.Gas[2:], 16)
		if gasVal != nil {
			detail.Gas = gasVal.Uint64()
		}
	}

	// Parse input
	if len(tx.Input) > 2 {
		detail.Input, _ = hex.DecodeString(tx.Input[2:])
	}

	// Parse type
	if len(tx.Type) > 2 {
		typeVal, _ := new(big.Int).SetString(tx.Type[2:], 16)
		if typeVal != nil {
			detail.Type = typeVal.Uint64()
		}
	}

	// Parse chainId
	if len(tx.ChainID) > 2 {
		detail.ChainID = new(big.Int)
		detail.ChainID.SetString(tx.ChainID[2:], 16)
	}

	// Parse nonce
	if len(tx.Nonce) > 2 {
		nonceVal, _ := new(big.Int).SetString(tx.Nonce[2:], 16)
		if nonceVal != nil {
			detail.Nonce = nonceVal.Uint64()
		}
	}

	// Parse gasPrice
	if len(tx.GasPrice) > 2 {
		detail.GasPrice = new(big.Int)
		detail.GasPrice.SetString(tx.GasPrice[2:], 16)
	}

	return detail, nil
}

// jsonTxDetail represents a transaction in JSON-RPC response
type jsonTxDetail struct {
	Hash     string  `json:"hash"`
	From     string  `json:"from"`
	To       *string `json:"to"`
	Value    string  `json:"value"`
	Gas      string  `json:"gas"`
	Input    string  `json:"input"`
	Type     string  `json:"type"`
	ChainID  string  `json:"chainId"`
	Nonce    string  `json:"nonce"`
	GasPrice string  `json:"gasPrice"`
}

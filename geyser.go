package yellowstone_geyser

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"
	"time"

	yellowstone_geyser_pb "github.com/blockchain-develop/yellowstone/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	ctx            context.Context
	conn           *grpc.ClientConn
	keepAliveCount int32
	geyserClient   yellowstone_geyser_pb.GeyserClient
}

func Connect(ctx context.Context, endpoint string, token string) (*Client, error) {
	if token != "" {
		meta := metadata.New(map[string]string{"x-token": token})
		ctx = metadata.NewOutgoingContext(ctx, meta)
	}

	var opts []grpc.DialOption
	myUrl, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	port := myUrl.Port()
	if port == "" {
		port = "443"
	}
	if myUrl.Scheme == "http" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else { // https
		pool, _ := x509.SystemCertPool()
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, "")))
	}
	hostname := myUrl.Hostname()
	if hostname == "" {
		return nil, errors.New("please provide URL format endpoint e.g. http(s)://<endpoint>:<port>")
	}

	address := hostname + ":" + port
	opts = append(opts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		))

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}
	geyserClient := yellowstone_geyser_pb.NewGeyserClient(conn)
	client := &Client{
		ctx:          ctx,
		conn:         conn,
		geyserClient: geyserClient,
	}
	// 连接已经建立，可以keep live
	go client.keepAlive()
	return client, nil
}

func (c *Client) SubscribeTransaction(req *yellowstone_geyser_pb.SubscribeRequestFilterTransactions) (yellowstone_geyser_pb.Geyser_SubscribeClient, error) {
	commitLevel := yellowstone_geyser_pb.CommitmentLevel_PROCESSED
	request := &yellowstone_geyser_pb.SubscribeRequest{
		Transactions: map[string]*yellowstone_geyser_pb.SubscribeRequestFilterTransactions{
			"test": req,
		},
		Commitment: &commitLevel,
	}
	stream, err := c.geyserClient.Subscribe(c.ctx)
	if err != nil {
		return nil, err
	}
	err = stream.Send(request)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Close closes the client and all the streams.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Ping(count int32) (*yellowstone_geyser_pb.PongResponse, error) {
	return c.geyserClient.Ping(c.ctx, &yellowstone_geyser_pb.PingRequest{Count: count})
}

func (c *Client) keepAlive() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			c.keepAliveCount++
			state := c.conn.GetState()
			switch state {
			case connectivity.Idle, connectivity.Ready:
				_, err := c.geyserClient.Ping(
					c.ctx,
					&yellowstone_geyser_pb.PingRequest{
						Count: c.keepAliveCount,
					},
				)
				// 如果ping失败，那么就直接关闭连接，让所有的strean都退出s，同时keepLive也退出
				// 这样上层在退出后，可以选择重连 & 重订阅
				if err != nil {
					c.conn.Close()
					return
				}
			default:
				// 连接状态异常，直接退出
				c.conn.Close()
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

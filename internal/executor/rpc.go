package executor

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/mbeoliero/scheduler/domain/entity"
)

type RpcExecutor struct{}

func NewRpcExecutor() *RpcExecutor {
	return &RpcExecutor{}
}

func (e *RpcExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*Result, error) {
	if config.Rpc == nil {
		return nil, fmt.Errorf("rpc payload is nil")
	}

	rpcConfig := config.Rpc

	// 连接到 gRPC 服务
	conn, err := grpc.NewClient(rpcConfig.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to connect: %v", err),
		}, err
	}
	defer conn.Close()

	// 设置 context metadata
	if len(rpcConfig.CtxKvs) > 0 {
		md := metadata.New(nil)
		for k, v := range rpcConfig.CtxKvs {
			md.Set(k, fmt.Sprint(v))
		}
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// 解析请求 JSON
	var reqData map[string]interface{}
	if rpcConfig.ReqJson != "" {
		if err := json.Unmarshal([]byte(rpcConfig.ReqJson), &reqData); err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Sprintf("failed to parse request json: %v", err),
			}, err
		}
	}

	// 调用 RPC 方法
	// 注意：这里使用 Invoke 进行通用调用
	var respData interface{}
	err = conn.Invoke(ctx, rpcConfig.Method, reqData, &respData)
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("rpc call failed: %v", err),
		}, err
	}

	// 序列化响应
	respJson, err := json.Marshal(respData)
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal response: %v", err),
		}, err
	}

	return &Result{
		Success: true,
		Output:  string(respJson),
	}, nil
}

package scheduler

import "github.com/mbeoliero/scheduler/infra/config"

func GetNodeId() string {
	return config.Get().Server.NodeId
}

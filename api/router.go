package api

import (
	"github.com/cloudwego/hertz/pkg/app/server"
)

// RegisterRoutes 注册所有路由
func RegisterRoutes(h *server.Hertz) {
	handler := NewJobHandler()

	// 健康检查
	h.GET("/health", handler.HealthCheck)

	// API v1
	v1 := h.Group("/api/v1")
	{
		// 任务管理
		jobs := v1.Group("/jobs")
		{
			jobs.POST("", handler.CreateJob)       // 创建任务
			jobs.PUT("", handler.UpdateJob)        // 更新任务
			jobs.GET("/:id", handler.GetJob)       // 查询任务
			jobs.DELETE("/:id", handler.DeleteJob) // 删除任务
			jobs.POST("/:id/pause", handler.PauseJob)   // 暂停任务
			jobs.POST("/:id/resume", handler.ResumeJob) // 恢复任务
		}

		// 任务执行记录
		records := v1.Group("/records")
		{
			records.GET("/:id", handler.GetJobRecord) // 查询执行记录
		}
	}
}

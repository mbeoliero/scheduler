package api

import (
	"context"
	"errors"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/service"
)

type JobHandler struct {
	jobService *service.JobService
}

func NewJobHandler() *JobHandler {
	return &JobHandler{
		jobService: service.NewJobService(),
	}
}

// Response 统一响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// CreateJobRequest 创建任务请求
type CreateJobRequest struct {
	Namespace       string               `json:"namespace" binding:"required"`
	JobKey          string               `json:"job_key" binding:"required"`
	Desc            string               `json:"desc"`
	ScheduleType    entity.ScheduleType  `json:"schedule_type" binding:"required"`
	ScheduleExpr    string               `json:"schedule_expr"`
	ExecuteType     entity.ExecuteType   `json:"execute_type" binding:"required"`
	Payload         *entity.JobPayload   `json:"payload" binding:"required"`
	Extra           *entity.JobExtraInfo `json:"extra"`
	CreatedBy       string               `json:"created_by"`
	NextTriggerTime int64                `json:"next_trigger_time,omitempty"` // 用于延迟任务
}

// UpdateJobRequest 更新任务请求
type UpdateJobRequest struct {
	Id              uint64               `json:"id" binding:"required"`
	Namespace       string               `json:"namespace" binding:"required"`
	JobKey          string               `json:"job_key" binding:"required"`
	Desc            string               `json:"desc"`
	ScheduleType    entity.ScheduleType  `json:"schedule_type" binding:"required"`
	ScheduleExpr    string               `json:"schedule_expr"`
	ExecuteType     entity.ExecuteType   `json:"execute_type" binding:"required"`
	Payload         *entity.JobPayload   `json:"payload" binding:"required"`
	Extra           *entity.JobExtraInfo `json:"extra"`
	NextTriggerTime int64                `json:"next_trigger_time,omitempty"`
}

// CreateJob 创建任务
func (h *JobHandler) CreateJob(ctx context.Context, c *app.RequestContext) {
	var req CreateJobRequest
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	job := &entity.Job{
		Namespace:       req.Namespace,
		JobKey:          req.JobKey,
		Desc:            req.Desc,
		ScheduleType:    req.ScheduleType,
		ScheduleExpr:    req.ScheduleExpr,
		ExecuteType:     req.ExecuteType,
		Payload:         req.Payload,
		Extra:           req.Extra,
		CreatedBy:       req.CreatedBy,
		NextTriggerTime: req.NextTriggerTime,
	}

	if err := h.jobService.CreateJob(ctx, job); err != nil {
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to create job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
		Data:    job,
	})
}

// UpdateJob 更新任务
func (h *JobHandler) UpdateJob(ctx context.Context, c *app.RequestContext) {
	var req UpdateJobRequest
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid request: " + err.Error(),
		})
		return
	}

	job := &entity.Job{
		Id:              req.Id,
		Namespace:       req.Namespace,
		JobKey:          req.JobKey,
		Desc:            req.Desc,
		ScheduleType:    req.ScheduleType,
		ScheduleExpr:    req.ScheduleExpr,
		ExecuteType:     req.ExecuteType,
		Payload:         req.Payload,
		Extra:           req.Extra,
		NextTriggerTime: req.NextTriggerTime,
	}

	if err := h.jobService.UpdateJob(ctx, job); err != nil {
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to update job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
		Data:    job,
	})
}

// GetJob 查询任务
func (h *JobHandler) GetJob(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid job id",
		})
		return
	}

	job, err := h.jobService.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			c.JSON(consts.StatusNotFound, Response{
				Code:    consts.StatusNotFound,
				Message: "job not found",
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to get job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
		Data:    job,
	})
}

// DeleteJob 删除任务
func (h *JobHandler) DeleteJob(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid job id",
		})
		return
	}

	if err = h.jobService.DeleteJob(ctx, id); err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			c.JSON(consts.StatusNotFound, Response{
				Code:    consts.StatusNotFound,
				Message: "job not found",
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to delete job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
	})
}

// PauseJob 暂停任务
func (h *JobHandler) PauseJob(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid job id",
		})
		return
	}

	if err = h.jobService.PauseJob(ctx, id); err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			c.JSON(consts.StatusNotFound, Response{
				Code:    consts.StatusNotFound,
				Message: "job not found",
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to pause job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
	})
}

// ResumeJob 恢复任务
func (h *JobHandler) ResumeJob(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid job id",
		})
		return
	}

	if err = h.jobService.ResumeJob(ctx, id); err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			c.JSON(consts.StatusNotFound, Response{
				Code:    consts.StatusNotFound,
				Message: "job not found",
			})
			return
		}
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to resume job: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
	})
}

// GetJobRecord 查询任务执行记录
func (h *JobHandler) GetJobRecord(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		c.JSON(consts.StatusBadRequest, Response{
			Code:    consts.StatusBadRequest,
			Message: "invalid record id",
		})
		return
	}

	record, err := h.jobService.GetJobRecord(ctx, id)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, Response{
			Code:    consts.StatusInternalServerError,
			Message: "failed to get job record: " + err.Error(),
		})
		return
	}

	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "success",
		Data:    record,
	})
}

// HealthCheck 健康检查
func (h *JobHandler) HealthCheck(ctx context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, Response{
		Code:    consts.StatusOK,
		Message: "ok",
	})
}

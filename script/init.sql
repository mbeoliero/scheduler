CREATE TABLE `job` (
                       `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
                       `namespace` VARCHAR(128) NOT NULL COMMENT '命名空间',
                       `job_key` VARCHAR(128) NOT NULL COMMENT '任务唯一标识',
                       `desc` VARCHAR(255) DEFAULT '' COMMENT '任务描述',
                       `status` TINYINT NOT NULL COMMENT '任务状态',
                       `schedule_type` TINYINT NOT NULL COMMENT '调度类型',
                       `schedule_expr` VARCHAR(255) NOT NULL COMMENT '调度表达式',
                       `execute_type` TINYINT NOT NULL COMMENT '执行类型',
                       `payload` JSON DEFAULT NULL COMMENT '任务执行参数',
                       `extra` JSON DEFAULT NULL COMMENT '扩展信息',
                       `next_trigger_time` BIGINT NOT NULL COMMENT '下次触发时间(毫秒)',
                       `created_by` VARCHAR(64) NOT NULL COMMENT '创建人',
                       `created_at` BIGINT NOT NULL COMMENT '创建时间(毫秒)',
                       `updated_at` BIGINT NOT NULL COMMENT '更新时间(毫秒)',
                       PRIMARY KEY (`id`),
                       UNIQUE KEY `uk_namespace_job_key` (`namespace`, `job_key`),
                       KEY `idx_trigger_time_status` (`status`, `next_trigger_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务定义表';

CREATE TABLE `job_record` (
                              `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
                              `job_id` BIGINT UNSIGNED NOT NULL COMMENT '任务ID',
                              `start_time` BIGINT NOT NULL COMMENT '开始时间(毫秒)',
                              `end_time` BIGINT DEFAULT NULL COMMENT '结束时间(毫秒)',
                              `job_status` TINYINT NOT NULL COMMENT '执行状态',
                              `result` TEXT COMMENT '执行结果',
                              `created_at` BIGINT NOT NULL COMMENT '创建时间(毫秒)',
                              `updated_at` BIGINT NOT NULL COMMENT '更新时间(毫秒)',
                              PRIMARY KEY (`id`),
                              KEY `idx_job_id` (`job_id`),
                              KEY `idx_job_status` (`job_status`),
                              KEY `idx_start_time` (`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行记录表';
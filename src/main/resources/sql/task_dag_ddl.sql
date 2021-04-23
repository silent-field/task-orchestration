-- ----------------------------
-- Table structure for task_meta
-- ----------------------------
DROP TABLE IF EXISTS `task_meta`;
CREATE TABLE `task_meta` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(64) NOT NULL COMMENT '任务标识',
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT '任务名称',
  `description` varchar(255) DEFAULT '' COMMENT '任务描述',
  `task_group_id` varchar(50) NOT NULL COMMENT '任务分组标识',
  `upstream_tasks` varchar(255) DEFAULT '' COMMENT '上游任务标识集合',
  `version` int(11) NOT NULL DEFAULT 0 COMMENT '当前任务执行版本',
  `pre_version` int(11) NOT NULL DEFAULT 0 COMMENT '任务上一个执行版本',
  `upstream_version` varchar(255) DEFAULT '' COMMENT '触发任务时<上游任务标识:上游版本集合>集合',
  `extra` varchar(255) DEFAULT '' COMMENT '扩展信息',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_idx_task_sign` (`task_id`, `task_group_id`),
  KEY `inx_task_group_id` (`task_group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for task_execute_log
-- ----------------------------
DROP TABLE IF EXISTS `task_execute_log`;
CREATE TABLE `task_execute_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `dag` varchar(1024) NOT NULL COMMENT 'dag图',
  `task_id` varchar(64) NOT NULL COMMENT '任务标识',
  `task_group_id` varchar(64) NOT NULL COMMENT '任务分组标识',
  `version` int(11) NOT NULL DEFAULT 0 COMMENT '当前任务执行版本',
  `pre_version` int(11) NOT NULL DEFAULT 0 COMMENT '任务上一个执行版本',
  `upstream_version` varchar(255) DEFAULT '' COMMENT '上游任务标识以及触发任务时上游版本集合',
  `scene` int(11) NOT NULL DEFAULT 0 COMMENT '触发场景',
  `xxl_params` varchar(1024) NOT NULL COMMENT 'xxl参数',
  `execute_params` varchar(1024) NOT NULL COMMENT '执行的实际参数',
  `start_time` bigint(20) NOT NULL DEFAULT 0 COMMENT '任务执行开始时间',
  `end_time` bigint(20) NOT NULL DEFAULT 0 COMMENT '任务执行完成时间',
  `cost_time` bigint(20) NOT NULL DEFAULT 0 COMMENT '耗时',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for task_pool
-- ----------------------------
DROP TABLE IF EXISTS `task_pool`;
CREATE TABLE `task_pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(64) NOT NULL COMMENT '任务标识',
  `task_group_id` varchar(50) NOT NULL COMMENT '任务分组标识',
  `upstream_tasks` varchar(255) DEFAULT '' COMMENT '上游任务标识集合',
  `version` int(11) NOT NULL DEFAULT 0 COMMENT '当前任务执行版本',
  `upstream_version` varchar(255) DEFAULT '' COMMENT '上游任务标识以及触发任务时上游版本集合',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for makeup_task_pool
-- ----------------------------
DROP TABLE IF EXISTS `makeup_task_pool`;
CREATE TABLE `makeup_task_pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `makeup_version` int(11) NOT NULL DEFAULT 0 COMMENT '补偿任务版本',
  `task_id` varchar(64) NOT NULL COMMENT '任务标识',
  `task_group_id` varchar(50) NOT NULL COMMENT '任务分组标识',
  `upstream_task` varchar(255) DEFAULT '' COMMENT '触发补偿的上游任务标识',
  `status` int(11) NOT NULL DEFAULT 0 COMMENT '状态',
  `makeup_params` varchar(1024) NOT NULL COMMENT '补偿参数',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for makeup_record
-- ----------------------------
DROP TABLE IF EXISTS `makeup_record`;
CREATE TABLE `makeup_record` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(64) NOT NULL COMMENT '被触发补偿的任务标识',
  `task_group_id` varchar(50) NOT NULL COMMENT '被触发补偿的任务分组标识',
  `downstream_makeup_tasks` varchar(255) DEFAULT '' COMMENT '需要补偿的下游任务标识集合',
  `makeup_params` varchar(1024) NOT NULL COMMENT '补偿参数',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

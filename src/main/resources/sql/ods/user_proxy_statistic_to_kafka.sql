-- ==========================================
-- ODS层：代理统计数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.user_proxy_statistic (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_user_proxy_statistic_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_user_proxy_statistic (Kafka)
--
-- 上游表：MySQL.user_proxy_statistic (CDC) - 业务库直连
-- 下游表：flink_user_proxy_statistic_data_sync (Kafka)
-- 下游文件：dwd/user_proxy_statistic_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 鍘熼厤缃紙浠呯敤浜庡紑鍙戯級
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 淇敼涓猴細浣跨敤OSS鍒嗗竷寮忓瓨鍌?
-- 杩涢樁浼樺寲閰嶇疆
SET 'table.exec.resource.default-parallelism' = '4';

-- 榛樿骞惰搴︼紝鏍规嵁 CPU 鏍稿績鏁拌皟鏁?SET 'execution.checkpointing.min-pause' = '1min';

-- 纭繚涓や釜 CP 涔嬮棿鏈夐棿姝囷紝鍑忚交瀛樺偍鍘嬪姏
-- 1. 瀹氫箟婧愯〃 (1024寮犲垎琛? 鍙澧為噺)
CREATE TABLE source_user_proxy_statistic (
`id` BIGINT,
    `user_id` BIGINT,
    `channel_id` BIGINT,
    `statistic_time` DATE,
    `direct_bet_rebate_amount` DECIMAL(20, 4),
    `direct_recharge_rebate_amount` DECIMAL(20, 4),
    `teams_bet_rebate_amount` DECIMAL(20, 4),
    `teams_recharge_rebate_amount` DECIMAL(20, 4),
    `team_size` INT,
    `direct_member` INT,
    `valid_direct_member` INT,
    `transfer_amount` DECIMAL(20, 4),
    `transfer_count` INT,
    `invitation_amount` DECIMAL(20, 4),
    `achievement_amount` DECIMAL(20, 4),
    `rebate_settlement` DECIMAL(20, 4),
    `betting_rebate_count` INT,
    `recharge_rebate_count` INT,
    `effective_bet_amount` DECIMAL(20, 4),
    `team_effective_bet_amount` DECIMAL(20, 4),
    `direct_effective_bet_amount` DECIMAL(20, 4),
    `teams_rebate_amount` DECIMAL(20, 4),
    `obtain_betting_rebate_count` INT,
    `obtain_recharge_rebate_count` INT,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(6),
    -- Flink 鍐呴儴鐘舵€佸幓閲嶄富閿紝閫氬父寤鸿鐢ㄧ墿鐞嗚〃涓婚敭
    PRIMARY KEY (`id`) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = 'pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com',
    'port' = '3306',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    'database-name' = 'assetscenter-test',
    'table-name' = 'user_proxy_statistic',
    'server-id' = '7501-7520', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Singapore',
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE sink_kafka (
`id` BIGINT,
    `user_id` BIGINT,
    `channel_id` BIGINT,
    `statistic_time` DATE,
    `direct_bet_rebate_amount` DECIMAL(20, 4),
    `direct_recharge_rebate_amount` DECIMAL(20, 4),
    `teams_bet_rebate_amount` DECIMAL(20, 4),
    `teams_recharge_rebate_amount` DECIMAL(20, 4),
    `team_size` INT,
    `direct_member` INT,
    `valid_direct_member` INT,
    `transfer_amount` DECIMAL(20, 4),
    `transfer_count` INT,
    `invitation_amount` DECIMAL(20, 4),
    `achievement_amount` DECIMAL(20, 4),
    `rebate_settlement` DECIMAL(20, 4),
    `betting_rebate_count` INT,
    `recharge_rebate_count` INT,
    `effective_bet_amount` DECIMAL(20, 4),
    `team_effective_bet_amount` DECIMAL(20, 4),
    `direct_effective_bet_amount` DECIMAL(20, 4),
    `teams_rebate_amount` DECIMAL(20, 4),
    `obtain_betting_rebate_count` INT,
    `obtain_recharge_rebate_count` INT,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(6)
)
WITH
  (
    'connector' = 'kafka',
    'topic' = 'flink_user_proxy_statistic_data_sync',
    'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
    -- 浣跨敤 debezium-json 鏍煎紡鍙互瀹岀編瑙ｆ瀽 MySQL CDC 鐨勬暟鎹祦
    'format' = 'debezium-json',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
  );

-- 3. 鎻愪氦浠诲姟
INSERT INTO
  sink_kafka
SELECT
  *
FROM
  source_user_proxy_statistic;

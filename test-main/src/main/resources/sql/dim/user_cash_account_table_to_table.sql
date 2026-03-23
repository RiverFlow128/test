-- ==========================================
-- DIM层：用户现金账户维度表同步
-- 职责：从MySQLCDC读取用户现金账户数据，同步到PolarDB维度表
--
-- 数据血缘：
--   MySQL.user_cash_account (业务库)
--       ↓ CDC [本文件]
--   DIM: user_cash_account (PolarDB statics库)
--
-- 上游表：MySQL.user_cash_account - 业务库直连
-- 上游文件：无（业务库CDC接入）
-- 下游表：user_cash_account (PolarDB)
-- 下游文件：无（维度表供查询使用）
-- ==========================================

-- 基础运行配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 进阶优化配置
SET 'table.exec.resource.default-parallelism' = '4';

-- 默认并行度，根据 CPU 核心数调整
SET 'execution.checkpointing.min-pause' = '1min';


CREATE TABLE user_cash_account_source (
    id BIGINT,
    user_id BIGINT,
    currency_type STRING,
    freeze_deposit_amount DECIMAL(19,4),
    freeze_withdrawal_amount DECIMAL(19,4),
    account_status STRING,
    first_recharge_time TIMESTAMP(3),
    last_recharge_time TIMESTAMP(3),
    first_withdrawal_time TIMESTAMP(3),
    last_withdrawal_time TIMESTAMP(3),
    withdraw_flag TINYINT,
    deposit_amount DECIMAL(19,4),
    withdrawal_amount DECIMAL(19,4),
    total_recharge_amount DECIMAL(19,4),
    total_bet_amount DECIMAL(19,4),
    total_withdrawal_amount DECIMAL(19,4),
    first_recharge_amount DECIMAL(19,4),
    first_withdrawal_amount DECIMAL(19,4),
    bonus_amount DECIMAL(19,4),
    freeze_bonus_amount DECIMAL(19,4),
    bonus_amount_expire_time TIMESTAMP(3),
    deposit_flow_target_amount DECIMAL(19,4),
    deposit_flow_amount DECIMAL(19,4),
    bonus_flow_target_amount DECIMAL(19,4),
    bonus_flow_amount DECIMAL(19,4),
    recharge_count INT,
    withdrawal_count INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(6),
    channel_id BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH 
  (
    'connector' = 'mysql-cdc',
    'hostname' = 'pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com',
    'port' = '3306',
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    'database-name' = 'user_basic-test',
    'table-name' = 'user_cash_account',
    'server-id' = '6601-6620', -- 确保范围内有足够的 server-id 分配给并行任务
    'server-time-zone' = 'Asia/Singapore',
    'scan.startup.mode' = 'initial'
  );


CREATE TABLE user_cash_account_taget (
    id BIGINT,
    user_id BIGINT,
    currency_type STRING,
    freeze_deposit_amount DECIMAL(19,4),
    freeze_withdrawal_amount DECIMAL(19,4),
    account_status STRING,
    first_recharge_time TIMESTAMP(3),
    last_recharge_time TIMESTAMP(3),
    first_withdrawal_time TIMESTAMP(3),
    last_withdrawal_time TIMESTAMP(3),
    withdraw_flag TINYINT,
    deposit_amount DECIMAL(19,4),
    withdrawal_amount DECIMAL(19,4),
    total_recharge_amount DECIMAL(19,4),
    total_bet_amount DECIMAL(19,4),
    total_withdrawal_amount DECIMAL(19,4),
    first_recharge_amount DECIMAL(19,4),
    first_withdrawal_amount DECIMAL(19,4),
    bonus_amount DECIMAL(19,4),
    freeze_bonus_amount DECIMAL(19,4),
    bonus_amount_expire_time TIMESTAMP(3),
    deposit_flow_target_amount DECIMAL(19,4),
    deposit_flow_amount DECIMAL(19,4),
    bonus_flow_target_amount DECIMAL(19,4),
    bonus_flow_amount DECIMAL(19,4),
    recharge_count INT,
    withdrawal_count INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(6),
    channel_id BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
)WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'user_cash_account',
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    -- 1. 开启缓冲写入（注意删除多余的反引号）
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 写入并发控制
    'sink.parallelism' = '3',
    -- 3. 重试策略（删除 sink. 前缀）
    'connection.max-retry-timeout' = '60s'
  );

-- 3. 提交任务
INSERT INTO
  user_cash_account_taget
SELECT
  *
FROM
  user_cash_account_source;
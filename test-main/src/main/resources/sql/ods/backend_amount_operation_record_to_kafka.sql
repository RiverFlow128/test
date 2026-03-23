-- ==========================================
-- ODS层：人工增减记录数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.amount_operation_record (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_amount_operation_record_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_amount_operation_record (Kafka)
--
-- 上游表：MySQL.amount_operation_record (CDC) - 业务库直连
-- 下游表：flink_amount_operation_record_data_sync (Kafka)
-- 下游文件：dwd/amount_operation_record_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 杩涢樁浼樺寲閰嶇疆
SET 'table.exec.resource.default-parallelism' = '4';

-- 榛樿骞惰搴︼紝鏍规嵁 CPU 鏍稿績鏁拌皟鏁?SET 'execution.checkpointing.min-pause' = '1min';

-- 纭繚涓や釜 CP 涔嬮棿鏈夐棿姝囷紝鍑忚交瀛樺偍鍘嬪姏
-- 1. 瀹氫箟婧愯〃 (1024寮犲垎琛? 鍙澧為噺)
CREATE TABLE source_amount_operation_record (
id BIGINT,
    user_id BIGINT,
    user_phone STRING,
    user_email STRING,
    nick_name STRING,
    `type` STRING,               -- type 鏄叧閿瓧锛屽缓璁敤鍙嶅紩鍙?    receive_type STRING,
    amount_type STRING,
    reward_title STRING,
    amount DECIMAL(19, 4),
    flow_amount DECIMAL(19, 4),
    balance DECIMAL(19, 4),
    withdraw_balance DECIMAL(19, 4),
    deposit_balance DECIMAL(19, 4),
    bonus_balance DECIMAL(19, 4),
    bonus_balance_expire_time TIMESTAMP(3),
    flow_balance DECIMAL(19, 4),
    deposit_flow_balance DECIMAL(19, 4),
    bonus_flow_balance DECIMAL(19, 4),
    status STRING,
    remark STRING,
    create_by STRING,
    update_by STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    channel_id BIGINT,
    auditor STRING,
    audit_remark STRING,
    audit_time TIMESTAMP(3),
    import_id BIGINT,
    show_flag TINYINT,
-- 銆愬叧閿慨鏀广€戞槧灏?row_kind 鍏冩暟鎹?    -- 瀹冧細杩斿洖璇稿 '+I' (Insert), '+U' (Update), '-D' (Delete) 绛夊€?    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
)
WITH
  (
    'connector' = 'mysql-cdc',
    'hostname' = 'pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com',
    'port' = '3306',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    'database-name' = 'admincenter-test',
    'table-name' = 'amount_operation_record',
    'server-id' = '6561-6580', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Singapore',
    'scan.startup.mode' = 'latest-offset' -- 只读增量
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE sink_kafka (
id BIGINT,
    user_id BIGINT,
    `type` STRING,               -- 澧炲姞锛欼NCREASE, 鍑忓皯锛歊EDUCE
    amount_type STRING,          -- WITHDRAW, DEPOSIT, BONUS
    amount DECIMAL(19, 4),
    balance DECIMAL(19, 4),
    status STRING,               -- COMPLETE
    create_by STRING,
    update_by STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    channel_id BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_amount_operation_record_data_sync',
    'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
    -- 浣跨敤 debezium-json 鏍煎紡鍙互瀹岀編瑙ｆ瀽 MySQL CDC 鐨勬暟鎹祦
    'format' = 'debezium-json', 
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- 3. 鎻愪氦浠诲姟
INSERT INTO sink_kafka -- 鍋囪浣犲凡缁忓畾涔変簡瀵瑰簲鐨?JDBC/ClickHouse Sink 琛?SELECT 
    id,
    user_id,
    `type`,
    amount_type,
    amount,
    balance,
    status,
    create_by,
    update_by,
    create_time,
    update_time,
    channel_id
FROM source_amount_operation_record
-- TODO: 淇敼鍘熷洜 - row_kind鍏冩暟鎹腑鐨勫垹闄ゆ爣璁版槸灏忓啓'd'鑰屼笉鏄?-D'
-- WHERE `row_status` <> '-D';
WHERE `row_status` <> 'd';

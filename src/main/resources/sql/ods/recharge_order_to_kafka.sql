-- ==========================================
-- ODS层：充值订单数据接入
-- 职责：从MySQLCDC读取原始数据，写入ODS层Kafka
--
-- 数据血缘：
--   MySQL.recharge_order (业务库)
--       ↓ CDC (debezium-json)
--   ODS: flink_recharge_order_data_sync (Kafka)
--       ↓ [本文件输出]
--   DWD: dwd_recharge_order (Kafka)
--
-- 上游表：MySQL.recharge_order (CDC) - 业务库直连
-- 下游表：flink_recharge_order_data_sync (Kafka)
-- 下游文件：dwd/recharge_order_dwd.sql
-- ==========================================

-- 任务配置
SET 'execution.checkpointing.interval' = '3min';

SET 'table.exec.sink.not-null-enforcer' = 'DROP';

--SET 'state.checkpoints.dir' = 'file:///opt/flink/data';

-- 杩涢樁浼樺寲閰嶇疆
SET 'table.exec.resource.default-parallelism' = '4';

-- 榛樿骞惰搴︼紝鏍规嵁 CPU 鏍稿績鏁拌皟鏁?SET 'execution.checkpointing.min-pause' = '1min';

-- 纭繚涓や釜 CP 涔嬮棿鏈夐棿姝囷紝鍑忚交瀛樺偍鍘嬪姏
CREATE TABLE recharge_order_source (
    id                           BIGINT,
    order_no                     BIGINT,
    user_id                      BIGINT,
    currency_code                STRING,
    rate_scale                   DECIMAL(20, 4),
    order_amount                 DECIMAL(20, 4),
    pay_amount                   DECIMAL(20, 4),
    decrease_handling_fee        DECIMAL(20, 4),
    handling_fee                 DECIMAL(20, 4),
    gift_amount                  DECIMAL(20, 4),
    recharge_amount              DECIMAL(20, 4),
    actual_handling_fee          DECIMAL(20, 4),
    actual_decrease_handling_fee DECIMAL(20, 4),
    actual_gift_amount           DECIMAL(20, 4),
    actual_recharge_amount       DECIMAL(20, 4),
    pay_status                   String,
    order_status                 String,
    first_recharge               TINYINT,
    pay_method_id                BIGINT,
    pay_category_id              BIGINT,
    pay_channel_id               BIGINT,
    pay_provider_id              BIGINT,
    recharge_success_time        TIMESTAMP(3),
    create_time                  TIMESTAMP(3),
    update_time                  TIMESTAMP(3),
    update_by                    STRING,
    channel_id                   BIGINT,
    `row_status` STRING METADATA FROM 'row_kind' VIRTUAL,     -- 銆愬叧閿慨澶嶃€戞槧灏?row_kind 鍏冩暟鎹紝瀹冧唬琛ㄦ搷浣滅被鍨?-- c: Create (Insert), u: Update, d: Delete, r: Read (Snapshot)
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
    'database-name' = 'ordercenter-test',
    'table-name' = 'recharge_order',
    'server-id' = '6521-6540', -- 确保配置内有足够的 server-id 分配给并行任务
    -- 统一时区配置为 Singapore 时区
    'server-time-zone' = 'Asia/Singapore',
    -- mysql-cdc中应设置为latest-offset表示从最新位置开始读
    'scan.startup.mode' = 'latest-offset'
  );

-- 2. 定义 Kafka Sink (内部 Kafka)
CREATE TABLE recharge_order_kafka_sink (
    id                           BIGINT,
    order_no                     BIGINT,
    user_id                      BIGINT,
    currency_code                STRING,
    rate_scale                   DECIMAL(20, 4),
    order_amount                 DECIMAL(20, 4),
    pay_amount                   DECIMAL(20, 4),
    decrease_handling_fee        DECIMAL(20, 4),
    handling_fee                 DECIMAL(20, 4),
    gift_amount                  DECIMAL(20, 4),
    recharge_amount              DECIMAL(20, 4),
    actual_handling_fee          DECIMAL(20, 4),
    actual_decrease_handling_fee DECIMAL(20, 4),
    actual_gift_amount           DECIMAL(20, 4),
    actual_recharge_amount       DECIMAL(20, 4),
    pay_status                   String,
    order_status                 String,
    first_recharge               TINYINT,
    pay_method_id                BIGINT,
    pay_category_id              BIGINT,
    pay_channel_id               BIGINT,
    pay_provider_id              BIGINT,
    recharge_success_time        TIMESTAMP(3),
    create_time                  TIMESTAMP(3),
    update_time                  TIMESTAMP(3),
    update_by                    STRING,
    channel_id                   BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_recharge_order_data_sync',
    'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
    -- 浣跨敤 debezium-json 鏍煎紡鍙互瀹岀編瑙ｆ瀽 MySQL CDC 鐨勬暟鎹祦
    'format' = 'debezium-json',
    'sink.partitioner' = 'round-robin',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- 3. 鎻愪氦浠诲姟
INSERT INTO
  recharge_order_kafka_sink
SELECT
  id,
  order_no,
  user_id,
  currency_code,
  rate_scale,
  order_amount,
  pay_amount,
  decrease_handling_fee,
  handling_fee,
  gift_amount,
  recharge_amount,
  actual_handling_fee,
  actual_decrease_handling_fee,
  actual_gift_amount,
  actual_recharge_amount,
  pay_status,
  order_status,
  first_recharge,
  pay_method_id,
  pay_category_id,
  pay_channel_id,
  pay_provider_id,
  recharge_success_time,
  create_time,
  update_time,
  update_by,
  channel_id
FROM
  recharge_order_source
-- TODO: 淇敼鍘熷洜 - row_kind鍏冩暟鎹腑鐨勫垹闄ゆ爣璁版槸'd'(灏忓啓)锛屼笉鏄?-D'銆?c':Insert, 'u':Update, 'd':Delete
WHERE `row_status` <> 'd';

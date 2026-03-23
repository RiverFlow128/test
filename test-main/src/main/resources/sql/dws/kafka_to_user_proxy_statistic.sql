-- ==========================================
-- DWS层：渠道代理统计聚合
-- 职责：从DWD层读取代理数据，按渠道聚合后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_user_proxy_statistic (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: today_channel_proxy_statistic (PolarDB)
--
-- 上游表：dwd_user_proxy_statistic (Kafka)
-- 上游文件：dwd/user_proxy_statistic_dwd.sql
-- 下游表：today_channel_proxy_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- 1. 核心配置
SET 'table.exec.mini-batch.enabled' = 'true';

-- 鍏佽 1 鍒嗛挓鐨勫欢杩燂紝杩欐剰鍛崇潃 Flink 浼氬湪鍐呭瓨涓敀 1 鍒嗛挓鐨勬暟鎹啀寰€ PolarDB 鍐欙紝杈惧埌闄嶆湰鐩殑
-- 减少mini-batch延迟，避免与checkpoint间隔冲突，防止数据积压
SET 'table.exec.mini-batch.allow-latency' = '1min';

-- 璁剧疆妯″紡涓?EXACTLY_ONCE
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 纭繚 Checkpoint 鍦ㄤ换鍔″彇娑堟椂淇濈暀锛屼互渚块噸鍚仮澶?SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- 鏀掓壒鐨勬渶澶ц鏁?SET 'table.exec.mini-batch.size' = '10000';

-- 寮€鍚姸鎬佹竻鐞嗭紝纭繚鍐呭瓨涓嶄細琚巻鍙叉暟鎹拺鐖嗭紙閽堝澶╃骇鎶ヨ〃锛岃缃?36 灏忔椂瓒冲锛?SET 'table.exec.state.ttl' = '36h';

-- 寮€鍚眬閮ㄨ仛鍚堜紭鍖栵紝瑙ｅ喅楂樺熀鏁颁笅鐨?COUNT DISTINCT 鎬ц兘闂
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 鍘熼厤缃紙浠呯敤浜庡紑鍙戯級
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 淇敼涓猴細浣跨敤OSS鍒嗗竷寮忓瓨鍌?
SET 'execution.checkpointing.interval' = '3min';

-- 2. 瀹氫箟DWD灞傛簮琛?(浠嶥WD灞侹afka璇诲彇娓呮礂鍚庣殑鏁版嵁)
CREATE TABLE dwd_user_proxy_statistic (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  id BIGINT,
  direct_bet_rebate_amount DECIMAL(20, 4),
  direct_recharge_rebate_amount DECIMAL(20, 4),
  teams_bet_rebate_amount DECIMAL(20, 4),
  teams_recharge_rebate_amount DECIMAL(20, 4),
  team_size INT,
  direct_member INT,
  valid_direct_member INT,
  transfer_amount DECIMAL(20, 4),
  transfer_count INT,
  invitation_amount DECIMAL(20, 4),
  achievement_amount DECIMAL(20, 4),
  rebate_settlement DECIMAL(20, 4),
  betting_rebate_count INT,
  recharge_rebate_count INT,
  effective_bet_amount DECIMAL(20, 4),
  team_effective_bet_amount DECIMAL(20, 4),
  direct_effective_bet_amount DECIMAL(20, 4),
  teams_rebate_amount DECIMAL(20, 4),
  obtain_betting_rebate_count INT,
  obtain_recharge_rebate_count INT,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(6),
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_proxy_statistic',
  'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
  'properties.group.id' = 'dws_user_proxy_statistic_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);

CREATE TABLE sink_today_channel_proxy_statistic (
  `channel_id` BIGINT,
  `statistic_time` DATE,
  `direct_bet_rebate_amount` DECIMAL(20, 4),
  `direct_recharge_rebate_amount` DECIMAL(20, 4),
  `teams_bet_rebate_amount` DECIMAL(20, 4),
  `teams_recharge_rebate_amount` DECIMAL(20, 4),
  `invitation_amount` DECIMAL(20, 4),
  `achievement_amount` DECIMAL(20, 4),
  `total_effective_bet_amount` DECIMAL(20, 4),
  `commissions_number` BIGINT,
  `valid_direct_member` BIGINT,
  `betting_rebate_count` BIGINT,
  `recharge_rebate_count` BIGINT,
  `betting_rebate_number` BIGINT,
  `recharge_rebate_number` BIGINT,
  `rebates_number` BIGINT,
  `total_transfer_amount` DECIMAL(20, 4),
  PRIMARY KEY (`statistic_time`, `channel_id`) NOT ENFORCED
)
WITH
  (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_proxy_statistic',
    -- TODO: 淇敼鍘熷洜 - 鏁忔劅淇℃伅涓嶅簲纭紪鐮侊紝鏀圭敤鐜鍙橀噺娉ㄥ叆
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    -- 1. 寮€鍚紦鍐插啓鍏ワ紙娉ㄦ剰鍒犻櫎澶氫綑鐨勫弽寮曞彿锛?    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    -- 2. 鍐欏叆骞跺彂鎺у埗
    'sink.parallelism' = '3',
    -- 3. 閲嶈瘯绛栫暐锛堝垹闄?sink. 鍓嶇紑锛?    'connection.max-retry-timeout' = '60s',
    
    -- TODO: 淇敼鍘熷洜 - JDBC Sink榛樿At-Least-Once锛屾棤娉曚繚璇丒xactly-Once璇箟
    -- 瑙ｅ喅鏂规锛氬湪INSERT璇彞涓娇鐢?INSERT ... ON DUPLICATE KEY UPDATE 瀹炵幇骞傜瓑鍐欏叆

  );

INSERT INTO
  sink_today_channel_proxy_statistic
SELECT
  channel_id,
  statistic_time,
  -- 閲戦绱姞
  SUM(direct_bet_rebate_amount) AS direct_bet_rebate_amount,
  SUM(direct_recharge_rebate_amount) AS direct_recharge_rebate_amount,
  SUM(teams_bet_rebate_amount) AS teams_bet_rebate_amount,
  SUM(teams_recharge_rebate_amount) AS teams_recharge_rebate_amount,
  SUM(invitation_amount) AS invitation_amount,
  SUM(achievement_amount) AS achievement_amount,
  SUM(effective_bet_amount) AS total_effective_bet_amount,
  -- 琚繑浣ｆ€讳汉鏁帮細鍥涢」浣ｉ噾涔嬪拰涓嶄负0鐨勪汉鏁板幓閲嶇粺璁?(Flink SQL 涓?COUNT 浣滅敤浜庡叿浣撳€兼椂浼氳嚜鍔ㄥ拷鐣?NULL)
  COUNT(
    CASE
      WHEN (
        direct_bet_rebate_amount + direct_recharge_rebate_amount + teams_bet_rebate_amount + teams_recharge_rebate_amount
      ) <> 0 THEN user_id
      ELSE NULL
    END
  ) AS commissions_number,
  SUM(valid_direct_member) AS valid_direct_member,
  -- 娆℃暟绱姞
  CAST(SUM(betting_rebate_count) AS BIGINT) AS betting_rebate_count,
  CAST(SUM(recharge_rebate_count) AS BIGINT) AS recharge_rebate_count,
  -- 鎶曟敞杩斾剑浜烘暟锛氭鏁颁笉涓?鐨勪汉鏁?  COUNT(
    CASE
      WHEN betting_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS betting_rebate_number,
  -- 鍏呭€艰繑浣ｄ汉鏁帮細娆℃暟涓嶄负0鐨勪汉鏁?  COUNT(
    CASE
      WHEN recharge_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS recharge_rebate_number,
  -- 杩斾剑鎬讳汉鏁帮細鎶曟敞鎴栧厖鍊艰繑浣ｆ鏁颁笉涓?鐨勪汉鏁?  COUNT(
    CASE
      WHEN betting_rebate_count <> 0
      OR recharge_rebate_count <> 0 THEN user_id
      ELSE NULL
    END
  ) AS rebates_number,
  SUM(transfer_amount) AS total_transfer_amount
FROM
  dwd_user_proxy_statistic
GROUP BY
  channel_id,
  statistic_time;

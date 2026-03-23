-- 1. 鏍稿績寰壒浼樺寲閰嶇疆
SET 'table.exec.mini-batch.enabled' = 'true';

-- 鍏佽 1 鍒嗛挓鐨勫欢杩燂紝杩欐剰鍛崇潃 Flink 浼氬湪鍐呭瓨涓敀 1 鍒嗛挓鐨勬暟鎹啀寰€ PolarDB 鍐欙紝杈惧埌闄嶆湰鐩殑
-- ==========================================
-- DWD层：用户在线状态记录
-- 职责：从ODS层Kafka读取用户在线状态数据，写入PolarDB
--
-- 数据血缘：
--   业务系统直写 (非FlinkCDC接入)
--       ↓
--   ODS: flink_user_online (Kafka)
--       ↓ Flink SQL [本文件]
--   DWD: user_online_status_record (PolarDB)
--
-- 上游表：flink_user_online (Kafka) - 业务系统直写
-- 上游文件：无（业务系统直接写入Kafka）
-- 下游表：user_online_status_record (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

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

-- 2. 瀹氫箟 Kafka Source 琛?-- 鏍稿績鎶€宸э細灏?jsonData 瀹氫箟涓?ROW 绫诲瀷锛屾柟渚垮悗缁€氳繃 . 璁块棶鍐呴儴瀛楁
CREATE TABLE kafka_source_user_online (
    `key` STRING,
    `dataSyncType` STRING,
    `jsonData` ROW<
        `user_id` BIGINT,
        `ip` STRING,
        `device_id` STRING,
        `online_time` STRING, -- 鍏堟槧灏勪负 STRING锛屽悗缁浆鎹㈡牸寮?        `create_time` STRING,
        `token` STRING,
        `session_id` STRING
    >
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_user_online', -- 璇峰～鍐欏疄闄?Topic
    'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
    'properties.group.id' = 'flink_user_online_group',
    'format' = 'json', -- 娉ㄦ剰锛氳繖閲岀殑鏍煎紡鏄爣鍑?JSON锛屼笉鏄?debezium-json
    'scan.startup.mode' = 'earliest-offset', -- 浠庝笂娆℃彁浜ょ殑鍋忕Щ閲忓紑濮嬭
    'properties.enable.auto.commit' = 'false' -- 绂佺敤 Kafka 鑷姩鎻愪氦锛屼氦缁?Flink 绠＄悊
);

-- 3. 瀹氫箟 MySQL Sink 琛?(StarRocks/Doris/MySQL 璇硶閫氱敤)
CREATE TABLE sink_user_online_status (
    `session_id` STRING,
    `user_id` BIGINT,
    `token` STRING,
    `online_time` TIMESTAMP(3),
    `ip` STRING,
    `device_id` STRING,
    `create_time` TIMESTAMP(3),
    PRIMARY KEY (`user_id`, `create_time`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'user_online_status_record',
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

-- 4. 鎵ц鍚屾浠诲姟
INSERT INTO sink_user_online_status
SELECT
    -- 鎻愬彇宓屽瀛楁骞跺鐞嗛粯璁ゅ€?    COALESCE(jsonData.session_id, 'N/A') as session_id,
    jsonData.user_id,
    jsonData.token,
    -- 澶勭悊鏃堕棿鏍煎紡杞崲 (ISO 8601 鏍煎紡 '2025-12-31T15:12:09' 鐩存帴鏀寔杞崲)
    TO_TIMESTAMP(REPLACE(jsonData.online_time, 'T', ' ')),
    jsonData.ip,
    jsonData.device_id,
    TO_TIMESTAMP(REPLACE(jsonData.create_time, 'T', ' '))
FROM kafka_source_user_online
WHERE jsonData.user_id IS NOT NULL
  AND jsonData.online_time IS NOT NULL
  AND jsonData.create_time IS NOT NULL;

-- ==========================================
-- DWS层：奖励记录聚合统计
-- 职责：从DWD层读取清洗后的数据，进行聚合计算后写入PolarDB
--
-- 数据血缘：
--   DWD: dwd_reward_record (Kafka)
--       ↓ Flink SQL 聚合 [本文件]
--   DWS: reward_record, today_user_reward_statistics_dws,
--        today_channel_reward_statistic, today_channel_activity_statistic (PolarDB)
--
-- 上游表：dwd_reward_record (Kafka)
-- 上游文件：dwd/reward_record_dwd.sql
-- 下游表：reward_record, today_user_reward_statistics_dws,
--         today_channel_reward_statistic, today_channel_activity_statistic (PolarDB)
-- 下游文件：无（终端表）
-- ==========================================

-- =========== 1. 全局资源与稳定性配置 ===========
SET 'execution.checkpointing.interval' = '3min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE'; -- 璁剧疆妯″紡涓?EXACTLY_ONCE
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
-- 蹇呴』淇敼涓哄垎甯冨紡鏂囦欢绯荤粺璺緞 - 宸叉槧灏勫涓绘満
--  SET 'state.checkpoints.dir' = 'file:///opt/flink/data';
-- 寤鸿寮€鍚閲?Checkpoint锛屽噺灏?50涓?QPS 涓嬬殑鐘舵€佸悓姝ュ帇鍔?SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION'; -- 确保 Checkpoint 在任务取消时保留，以便重启恢复
-- 生产环境必须配置分布式存储路径，本地文件路径在集群模式下无法恢复
-- SET 'state.checkpoints.dir' = 'file:///opt/flink/data'; -- 原配置（仅用于开发）
SET 'state.checkpoints.dir' = 'oss://your-bucket/flink-checkpoints/'; -- 修改为：使用OSS分布式存储
-- 统一时区配置为 Singapore 时区
-- SET 'table.local-time-zone' = 'Asia/Jakarta'; -- 原配置
SET 'table.local-time-zone' = 'Asia/Singapore'; -- 修改为：使用Singapore时区

-- =========== 2. 性能优化（应对高并发写入 PolarDB）===========
SET 'table.exec.mini-batch.enabled' = 'true'; -- 开启批处理，减少对数据库的访问频率
-- 批处理延迟应该 ≤ Checkpoint间隔/2，避免checkpoint触发时积压未提交数据
-- SET 'table.exec.mini-batch.allow-latency' = '3min'; -- 原配置（与checkpoint冲突）
SET 'table.exec.mini-batch.allow-latency' = '1min'; -- 修改为：1分钟 ≤ 3分钟Checkpoint间隔/2
SET 'table.exec.mini-batch.size' = '10000'; -- 开启批处理，减少对数据库的访问频率
SET 'table.exec.state.ttl' = '36h'; -- 开启状态清理，确保内存不会被历史数据撑爆（针对天级表，设置 36 小时足够）
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE'; -- 开启全局聚合优化，解决高并发下的 COUNT DISTINCT 性能问题
SET 'table.optimizer.distinct-agg.split.enabled' = 'true'; -- 开启去重聚合优化（针对你那几个 COUNT DISTINCT）

-- 2. 定义DWD层源表(从DWD层Kafka读取清洗后的数据)
CREATE TABLE dwd_reward_record (
  statistic_time DATE,
  user_id BIGINT,
  channel_id BIGINT,
  activity_type STRING,
  id BIGINT,
  unique_id BIGINT,
  reward_scope STRING,
  reward_type STRING,
  bonus_amount DECIMAL(20, 4),
  draw_flag INT,
  not_receive_time TIMESTAMP(3),
  receive_time TIMESTAMP(3),
  time_out_time TIMESTAMP(3),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  WATERMARK FOR update_time AS update_time - INTERVAL '5' MINUTE,
  PRIMARY KEY (id, update_time) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_reward_record',
  'properties.bootstrap.servers' = 'alikafka-pre-public-intl-sg-d1s4p7uvd01-1-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-public-intl-sg-d1s4p7uvd01-3-vpc.alikafka.aliyuncs.com:9092',
  'properties.group.id' = 'dws_reward_record_group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.enable.auto.commit' = 'false'
);


create table target_reward_record
(
    id               bigint          comment '涓婚敭ID',
    user_id          bigint          comment '鐢ㄦ埛ID',
    channel_id       bigint          comment '娓犻亾ID',
    unique_id        bigint          comment '濂栭噾璁板綍鍞竴缂栧彿',
    reward_scope     STRING          comment '濂栧姳鑼冨洿',
    reward_type      STRING          comment '濂栧姳绫诲瀷 锛?鏆傛椂鐢ㄤ笉鍒颁繚鐣欏瓧娈?,
    activity_type    STRING          comment '娲诲姩绫诲瀷:  OTHER-鍏朵粬,REFERRAL_REWARD-鎺ㄨ崘濂栧姳...',
    bonus_amount     decimal(20, 4)  comment '濂栭噾閲戦锛氭湰娆″鍔辩殑閲戦',
    -- 与生产环境保持一致，使用 INT 类型 (0: 未领取, 1: 已领取)
    draw_flag        INT         comment '领取状态 (0: 未领取, 1: 已领取)',
    not_receive_time TIMESTAMP(3)        comment '奖励逾期未领取记录时间',
    receive_time     TIMESTAMP(3)        comment '奖励领取时间',
    time_out_time    TIMESTAMP(3)        comment '过期时间',
    create_time      TIMESTAMP(3)        comment '创建时间',
    update_time      TIMESTAMP(3)        comment '更新时间',
    primary key (user_id, unique_id, create_time) NOT ENFORCED
)
WITH
    (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'reward_record',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    
    -- 1. 寮€鍚紦鍐插啓鍏ワ紙娉ㄦ剰鍒犻櫎澶氫綑鐨勫弽寮曞彿锛?    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 鍐欏叆骞跺彂鎺у埗
    'sink.parallelism' = '3',
    
    -- 3. 閲嶈瘯绛栫暐锛堝垹闄?sink. 鍓嶇紑锛?    'connection.max-retry-timeout' = '60s',
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 瑙ｅ喅鏂规锛氬湪INSERT璇彞涓娇鐢?INSERT ... ON DUPLICATE KEY UPDATE 瀹炵幇骞傜瓑鍐欏叆

    );






-- TODO: 淇敼鍘熷洜 - 娉ㄩ噴涓庡疄闄呴厤缃笉绗︼紝杩欐槸MySQL鑰岄潪PolarDB
-- 瀹氫箟 MySQL 鐩爣琛紙娉細铏界劧闇€姹傝PolarDB锛屽疄闄呰繛鎺ヤ负MySQL)
create table today_user_reward_statistics_dws
(
    -- 缁熻缁村害鍜屾爣璇?    statistic_time DATE NOT NULL comment '缁熻鏃ユ湡',
    user_id BIGINT NOT NULL comment '鐢ㄦ埛ID',
    channel_id BIGINT NOT NULL comment '娓犻亾ID',
    activity_type STRING NOT NULL comment '娲诲姩绫诲瀷锛堜富瑕佺敤浜庡吋瀹瑰師娲诲姩琛級',

    -- VIP绯荤粺缁熻
    vip_draw_count BIGINT comment 'VIP绯荤粺棰嗗彇绗旀暟',
    vip_no_draw_count BIGINT comment 'VIP绯荤粺寰呴鍙栫瑪鏁?,
    vip_draw_amount DECIMAL(20, 4) comment 'VIP绯荤粺棰嗗彇閲戦',
    vip_no_draw_amount DECIMAL(20, 4) comment 'VIP绯荤粺寰呴鍙栭噾棰?,
    vip_draw_total_withdrawal_amount DECIMAL(20, 4) comment 'VIP绯荤粺-鎬婚噾棰?鍙彁鐜伴噾棰?,
    vip_draw_total_deposit_amount DECIMAL(20, 4) comment 'VIP绯荤粺-鎬婚噾棰?閿佸畾浣欓',
    vip_draw_total_bonus_amount DECIMAL(20, 4) comment 'VIP绯荤粺-鎬婚噾棰?濂栭噾',
    vip_draw_withdrawal_amount DECIMAL(20, 4) comment 'VIP绯荤粺-宸查鍙?鍙彁鐜颁綑棰?,
    vip_draw_deposit_amount DECIMAL(20, 4) comment 'VIP绯荤粺-宸查鍙?閿佸畾浣欓',
    vip_draw_bonus_amount DECIMAL(20, 4) comment 'VIP绯荤粺-宸查鍙?濂栭噾',
    vip_no_draw_withdrawal_amount DECIMAL(20, 4) comment 'VIP绯荤粺-寰呴鍙?鍙彁鐜颁綑棰?,
    vip_no_draw_deposit_amount DECIMAL(20, 4) comment 'VIP绯荤粺-寰呴鍙?閿佸畾浣欓',
    vip_no_draw_bonus_amount DECIMAL(20, 4) comment 'VIP绯荤粺-寰呴鍙?濂栭噾',

    -- 杩旀按绯荤粺缁熻
    rebate_draw_count BIGINT comment '杩旀按棰嗗彇绗旀暟',
    rebate_no_draw_count BIGINT comment '杩旀按寰呴鍙栫瑪鏁?,
    rebate_draw_amount DECIMAL(20, 4) comment '杩旀按棰嗗彇閲戦',
    rebate_no_draw_amount DECIMAL(20, 4) comment '杩旀按寰呴鍙栭噾棰?,
    rebate_draw_total_withdrawal_amount DECIMAL(20, 4) comment '杩旀按-鎬婚噾棰?鍙彁鐜伴噾棰?,
    rebate_draw_total_deposit_amount DECIMAL(20, 4) comment '杩旀按-鎬婚噾棰?閿佸畾浣欓',
    rebate_draw_total_bonus_amount DECIMAL(20, 4) comment '杩旀按-鎬婚噾棰?濂栭噾',
    rebate_draw_withdrawal_amount DECIMAL(20, 4) comment '杩旀按-宸查鍙?鍙彁鐜颁綑棰?,
    rebate_draw_deposit_amount DECIMAL(20, 4) comment '杩旀按-宸查鍙?閿佸畾浣欓',
    rebate_draw_bonus_amount DECIMAL(20, 4) comment '杩旀按-宸查鍙?濂栭噾',
    rebate_no_draw_withdrawal_amount DECIMAL(20, 4) comment '杩旀按寰呴鍙?鍙彁鐜颁綑棰?,
    rebate_no_draw_deposit_amount DECIMAL(20, 4) comment '杩旀按寰呴鍙?閿佸畾浣欓',
    rebate_no_draw_bonus_amount DECIMAL(20, 4) comment '杩旀按寰呴鍙?濂栭噾',

    -- 娲诲姩缁熻
    activity_total_count BIGINT comment '娲诲姩鎬荤瑪鏁?,
    activity_draw_count BIGINT comment '娲诲姩棰嗗彇绗旀暟',
    activity_no_draw_count BIGINT comment '娲诲姩寰呴鍙栫瑪鏁?,
    activity_total_amount DECIMAL(20, 4) comment '娲诲姩鎬婚噾棰?,
    activity_draw_amount DECIMAL(20, 4) comment '娲诲姩棰嗗彇閲戦',
    activity_no_draw_amount DECIMAL(20, 4) comment '娲诲姩寰呴鍙栭噾棰?,
    activity_total_withdrawal_amount DECIMAL(20, 4) comment '娲诲姩濂栧姳鎬婚-鍙彁鐜?,
    activity_total_deposit_amount DECIMAL(20, 4) comment '娲诲姩濂栧姳鎬婚-閿佸畾',
    activity_total_bonus_amount DECIMAL(20, 4) comment '娲诲姩濂栧姳鎬婚-濂栭噾',
    activity_draw_withdrawal_amount DECIMAL(20, 4) comment '娲诲姩宸查-鍙彁鐜?,
    activity_draw_deposit_amount DECIMAL(20, 4) comment '娲诲姩宸查-閿佸畾',
    activity_draw_bonus_amount DECIMAL(20, 4) comment '娲诲姩宸查-濂栭噾',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) comment '娲诲姩鏈-鍙彁鐜?,
    activity_no_draw_deposit_amount DECIMAL(20, 4) comment '娲诲姩鏈-閿佸畾',
    activity_no_draw_bonus_amount DECIMAL(20, 4) comment '娲诲姩鏈-濂栭噾',

    -- 濂栭噾浠诲姟缁熻
    bonus_task_draw_count BIGINT comment '濂栭噾浠诲姟棰嗗彇绗旀暟',
    bonus_task_no_draw_count BIGINT comment '濂栭噾浠诲姟寰呴鍙栫瑪鏁?,
    bonus_task_draw_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟棰嗗彇閲戦',
    bonus_task_no_draw_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟寰呴鍙栭噾棰?,
    bonus_task_total_withdrawal_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-鎬婚噾棰?鍙彁鐜伴噾棰?,
    bonus_task_total_deposit_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-鎬婚噾棰?閿佸畾浣欓',
    bonus_task_total_bonus_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-鎬婚噾棰?濂栭噾',
    bonus_task_no_draw_withdrawal_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-寰呴鍙?鍙彁鐜伴噾棰?,
    bonus_task_no_draw_deposit_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-寰呴鍙?閿佸畾閲戦',
    bonus_task_no_draw_bonus_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-寰呴鍙?濂栭噾',
    bonus_task_draw_withdrawal_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-宸查鍙?鍙彁鐜伴噾棰?,
    bonus_task_draw_deposit_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-宸查鍙?閿佸畾閲戦',
    bonus_task_draw_bonus_amount DECIMAL(20, 4) comment '濂栭噾浠诲姟-宸查鍙?濂栭噾',

    -- 娓稿浣撻獙閲戠粺璁?    tourist_draw_count BIGINT comment '娓稿浣撻獙閲戦鍙栫瑪鏁?,
    tourist_no_draw_count BIGINT comment '娓稿浣撻獙閲戝緟棰嗗彇绗旀暟',
    tourist_draw_total_withdrawal_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?鎬婚噾棰?鍙彁鐜伴噾棰?,
    tourist_draw_total_deposit_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?鎬婚噾棰?閿佸畾浣欓',
    tourist_draw_total_bonus_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?鎬婚噾棰?濂栭噾',
    tourist_draw_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲戝凡棰嗗彇閲戦',
    tourist_draw_withdrawal_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?宸查鍙?鍙彁鐜颁綑棰?,
    tourist_draw_deposit_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?宸查鍙?閿佸畾浣欓',
    tourist_draw_bonus_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?宸查鍙?濂栭噾',
    tourist_no_draw_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?寰呴鍙?閲戦',
    tourist_no_draw_withdrawal_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?寰呴鍙?鍙彁鐜颁綑棰?,
    tourist_no_draw_deposit_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?寰呴鍙?閿佸畾浣欓',
    tourist_no_draw_bonus_amount DECIMAL(20, 4) comment '娓稿浣撻獙閲?寰呴鍙?濂栭噾',

    -- 涓婚敭瀹氫箟锛圢OT ENFORCED琛ㄧず涓嶅己鍒舵鏌ワ紝瀹為檯妫€鏌ョ敱鐩爣鏁版嵁搴撳畬鎴愶級
    PRIMARY KEY (statistic_time, user_id, channel_id, activity_type) NOT ENFORCED
)
WITH
    (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_user_reward_statistics_dws',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    
    -- 1. 寮€鍚紦鍐插啓鍏ワ紙娉ㄦ剰鍒犻櫎澶氫綑鐨勫弽寮曞彿锛?    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 鍐欏叆骞跺彂鎺у埗
    'sink.parallelism' = '3',
    
    -- 3. 閲嶈瘯绛栫暐锛堝垹闄?sink. 鍓嶇紑锛?    'connection.max-retry-timeout' = '60s',
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 瑙ｅ喅鏂规锛氬湪INSERT璇彞涓娇鐢?INSERT ... ON DUPLICATE KEY UPDATE 瀹炵幇骞傜瓑鍐欏叆

    );
-- 娓犻亾鐩爣琛?CREATE TABLE today_channel_reward_statistic (
    user_count BIGINT COMMENT '鐢ㄦ埛鎬绘暟',
    vip_user_count BIGINT COMMENT 'VIP濂栧姳鍙備笌浜烘暟',

    -- VIP绯荤粺缁熻
    vip_draw_count BIGINT COMMENT 'VIP绯荤粺棰嗗彇绗旀暟',
    vip_no_draw_count BIGINT COMMENT 'VIP绯荤粺寰呴鍙栫瑪鏁?,
    vip_draw_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺棰嗗彇閲戦',
    vip_no_draw_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺寰呴鍙栭噾棰?,
    vip_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-鎬婚噾棰?鍙彁鐜伴噾棰?,
    vip_draw_total_deposit_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-鎬婚噾棰?閿佸畾浣欓',
    vip_draw_total_bonus_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-鎬婚噾棰?濂栭噾',
    vip_draw_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-宸查鍙?鍙彁鐜颁綑棰?,
    vip_draw_deposit_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-宸查鍙?閿佸畾浣欓',
    vip_draw_bonus_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-宸查鍙?濂栭噾',
    vip_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-寰呴鍙?鍙彁鐜颁綑棰?,
    vip_no_draw_deposit_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-寰呴鍙?閿佸畾浣欓',
    vip_no_draw_bonus_amount DECIMAL(20, 4) COMMENT 'VIP绯荤粺-寰呴鍙?濂栭噾',

    -- 杩旀按绯荤粺缁熻
    rebate_user_count BIGINT COMMENT '杩旀按濂栧姳鍙備笌浜烘暟',
    rebate_draw_count BIGINT COMMENT '杩旀按棰嗗彇绗旀暟',
    rebate_no_draw_count BIGINT COMMENT '杩旀按寰呴鍙栫瑪鏁?,
    rebate_draw_amount DECIMAL(20, 4) COMMENT '杩旀按棰嗗彇閲戦',
    rebate_no_draw_amount DECIMAL(20, 4) COMMENT '杩旀按寰呴鍙栭噾棰?,
    rebate_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT '杩旀按-鎬婚噾棰?鍙彁鐜伴噾棰?,
    rebate_draw_total_deposit_amount DECIMAL(20, 4) COMMENT '杩旀按-鎬婚噾棰?閿佸畾浣欓',
    rebate_draw_total_bonus_amount DECIMAL(20, 4) COMMENT '杩旀按-鎬婚噾棰?濂栭噾',
    rebate_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '杩旀按-宸查鍙?鍙彁鐜颁綑棰?,
    rebate_draw_deposit_amount DECIMAL(20, 4) COMMENT '杩旀按-宸查鍙?閿佸畾浣欓',
    rebate_draw_bonus_amount DECIMAL(20, 4) COMMENT '杩旀按-宸查鍙?濂栭噾',
    rebate_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '杩旀按寰呴鍙?鍙彁鐜颁綑棰?,
    rebate_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '杩旀按寰呴鍙?閿佸畾浣欓',
    rebate_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '杩旀按寰呴鍙?濂栭噾',

    -- 娲诲姩缁熻
    activity_user_count BIGINT COMMENT '娲诲姩濂栧姳鍙備笌浜烘暟',
    activity_draw_count BIGINT COMMENT '娲诲姩棰嗗彇绗旀暟',
    activity_no_draw_count BIGINT COMMENT '娲诲姩寰呴鍙栫瑪鏁?,
    activity_total_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳-鎬婚噾棰?,
    activity_draw_amount DECIMAL(20, 4) COMMENT '娲诲姩棰嗗彇閲戦',
    activity_no_draw_amount DECIMAL(20, 4) COMMENT '娲诲姩寰呴鍙栭噾棰?,
    activity_total_withdrawal_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?閲戦-鍙彁鐜颁綑棰?,
    activity_total_deposit_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?閲戦-閿佸畾浣欓',
    activity_total_bonus_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?閲戦-濂栭噾',
    activity_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?鍙彁鐜颁綑棰?,
    activity_draw_deposit_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?閿佸畾浣欓',
    activity_draw_bonus_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?濂栭噾',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?鍙彁鐜颁綑棰?,
    activity_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?閿佸畾浣欓',
    activity_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?濂栭噾',

    -- 濂栭噾浠诲姟缁熻
    bonus_task_user_count BIGINT COMMENT '濂栭噾浠诲姟鍙備笌浜烘暟',
    bonus_task_draw_count BIGINT COMMENT '濂栭噾浠诲姟棰嗗彇绗旀暟',
    bonus_task_no_draw_count BIGINT COMMENT '濂栭噾浠诲姟寰呴鍙栫瑪鏁?,
    bonus_task_draw_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟棰嗗彇閲戦',
    bonus_task_no_draw_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟寰呴鍙栭噾棰?,
    bonus_task_total_withdrawal_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-鎬婚噾棰?鍙彁鐜伴噾棰?,
    bonus_task_total_deposit_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-鎬婚噾棰?閿佸畾浣欓',
    bonus_task_total_bonus_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-鎬婚噾棰?濂栭噾',
    bonus_task_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-寰呴鍙?鍙彁鐜伴噾棰?,
    bonus_task_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-寰呴鍙?閿佸畾閲戦',
    bonus_task_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-寰呴鍙?濂栭噾',
    bonus_task_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-宸查鍙?鍙彁鐜伴噾棰?,
    bonus_task_draw_deposit_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-宸查鍙?閿佸畾閲戦',
    bonus_task_draw_bonus_amount DECIMAL(20, 4) COMMENT '濂栭噾浠诲姟-宸查鍙?濂栭噾',

    -- 缁村害瀛楁
    channel_id BIGINT NOT NULL COMMENT '娓犻亾ID',
    statistic_time DATE NOT NULL COMMENT '缁熻鏃ユ湡',

    -- 娓稿浣撻獙閲戠粺璁?    tourist_draw_count BIGINT COMMENT '娓稿浣撻獙閲戦鍙栫瑪鏁?,
    tourist_no_draw_count BIGINT COMMENT '娓稿浣撻獙閲戝緟棰嗗彇绗旀暟',
    tourist_draw_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲戦鍙栭噾棰?,
    tourist_draw_total_withdrawal_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?鎬婚噾棰?鍙彁鐜伴噾棰?,
    tourist_draw_total_deposit_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?鎬婚噾棰?閿佸畾浣欓',
    tourist_draw_total_bonus_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?鎬婚噾棰?濂栭噾',
    tourist_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?宸查鍙?鍙彁鐜颁綑棰?,
    tourist_draw_deposit_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?宸查鍙?閿佸畾浣欓',
    tourist_draw_bonus_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?宸查鍙?濂栭噾',
    tourist_no_draw_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?寰呴鍙?閲戦',
    tourist_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?寰呴鍙?鍙彁鐜颁綑棰?,
    tourist_no_draw_deposit_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?寰呴鍙?閿佸畾浣欓',
    tourist_no_draw_bonus_amount DECIMAL(20, 4) COMMENT '娓稿浣撻獙閲?寰呴鍙?濂栭噾',

    -- 涓婚敭瀹氫箟
    PRIMARY KEY (statistic_time, channel_id) NOT ENFORCED
)
COMMENT '娓犻亾浠婃棩濂栧姳缁熻鏃ユ姤'
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_reward_statistic',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    
    -- 1. 寮€鍚紦鍐插啓鍏ワ紙娉ㄦ剰鍒犻櫎澶氫綑鐨勫弽寮曞彿锛?    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 鍐欏叆骞跺彂鎺у埗
    'sink.parallelism' = '3',
    
    -- 3. 閲嶈瘯绛栫暐锛堝垹闄?sink. 鍓嶇紑锛?    'connection.max-retry-timeout' = '60s',
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 瑙ｅ喅鏂规锛氬湪INSERT璇彞涓娇鐢?INSERT ... ON DUPLICATE KEY UPDATE 瀹炵幇骞傜瓑鍐欏叆

);



-- 鐩爣琛?鐢ㄦ埛濂栧姳姹囨€昏〃
CREATE TABLE today_channel_activity_statistic
(
    user_count                         BIGINT COMMENT '鐢ㄦ埛鎬绘暟',
    channel_id                         BIGINT NOT NULL COMMENT '娓犻亾ID',
    activity_type                      STRING NOT NULL COMMENT '娲诲姩绫诲瀷锛堝搴旀灇涓? ActivityTypeEnum锛?,
    statistic_time                     DATE   NOT NULL COMMENT '缁熻鏃ユ湡',
    view_activity_count                BIGINT COMMENT '鏌ョ湅娲诲姩浜烘暟',
    activity_reward_count              BIGINT COMMENT '鑾峰緱娲诲姩濂栧姳浜烘暟',
    activity_total_amount              DECIMAL(20, 4) COMMENT '娲诲姩鎬婚噾棰?,
    activity_user_count                BIGINT COMMENT '娲诲姩鐨勪汉鏁?,
    activity_draw_count                BIGINT COMMENT '娲诲姩棰嗗彇绗旀暟',
    activity_no_draw_count             BIGINT COMMENT '娲诲姩寰呴鍙栫瑪鏁?,
    activity_draw_amount               DECIMAL(20, 4) COMMENT '娲诲姩棰嗗彇閲戦',
    activity_no_draw_amount            DECIMAL(20, 4) COMMENT '娲诲姩寰呴鍙栭噾棰?,
    activity_total_withdrawal_amount   DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?鍙彁鐜颁綑棰?,
    activity_total_deposit_amount      DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?閿佸畾浣欓',
    activity_total_bonus_amount        DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鎬婚噾棰?濂栭噾',
    activity_draw_withdrawal_amount    DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?鍙彁鐜颁綑棰?,
    activity_draw_deposit_amount       DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?閿佸畾浣欓',
    activity_draw_bonus_amount         DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-宸查鍙栭噾棰?濂栭噾',
    activity_no_draw_withdrawal_amount DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?鍙彁鐜颁綑棰?,
    activity_no_draw_deposit_amount    DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?閿佸畾浣欓',
    activity_no_draw_bonus_amount      DECIMAL(20, 4) COMMENT '娲诲姩濂栧姳閲戦-鏈鍙栭噾棰?濂栭噾',

    -- 涓婚敭瀹氫箟锛堝拰MySQL琛ㄤ竴鑷达級
    PRIMARY KEY (statistic_time, activity_type, channel_id) NOT ENFORCED
) COMMENT '娓犻亾娲诲姩鏃ョ粺璁¤〃'
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://pe-gs5nabd84m1e7wkez.rwlb.singapore.rds.aliyuncs.com:3306/statics?rewriteBatchedStatements=true',
    'table-name' = 'today_channel_activity_statistic',
    -- 敏感信息不应硬编码，使用环境变量注入
    'username' = '${DB_USERNAME}',
    'password' = '${DB_PASSWORD}',
    
    -- 1. 寮€鍚紦鍐插啓鍏ワ紙娉ㄦ剰鍒犻櫎澶氫綑鐨勫弽寮曞彿锛?    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '5s',
    
    -- 2. 鍐欏叆骞跺彂鎺у埗
    'sink.parallelism' = '3',
    
    -- 3. 閲嶈瘯绛栫暐锛堝垹闄?sink. 鍓嶇紑锛?    'connection.max-retry-timeout' = '60s',
    
    -- JDBC Sink默认At-Least-Once，无法保证Exactly-Once语义
    -- 瑙ｅ喅鏂规锛氬湪INSERT璇彞涓娇鐢?INSERT ... ON DUPLICATE KEY UPDATE 瀹炵幇骞傜瓑鍐欏叆

);




-- 鐩爣琛ㄦ彃鍏?濂栧姳璁板綍鏄庣粏琛?INSERT INTO target_reward_record
SELECT
    id,
    user_id,
    channel_id,
    unique_id,
    reward_scope,
    reward_type,
    activity_type,
    bonus_amount,
    draw_flag,
    not_receive_time,
    receive_time,
    time_out_time,
    create_time,
    update_time
FROM dwd_reward_record
WHERE id IS NOT NULL;

-- 鐩爣琛ㄦ彃鍏?鐢ㄦ埛缁村害濂栧姳姹囨€昏〃
INSERT INTO today_user_reward_statistics_dws
SELECT
    statistic_time,
    user_id,
    channel_id,
    COALESCE(activity_type, '') AS activity_type,

    -- ============ VIP绯荤粺缁熻 ============
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN 1 END) AS vip_draw_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN 1 END) AS vip_no_draw_count,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS vip_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS vip_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_no_draw_bonus_amount,

    -- ============ 杩旀按绯荤粺缁熻 ============
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN 1 END) AS rebate_draw_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN 1 END) AS rebate_no_draw_count,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS rebate_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS rebate_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_bonus_amount,

    -- ============ 娲诲姩缁熻 (姝ゅ閫氳繃鍐呭眰鍘婚噸鍚堝苟閫昏緫) ============
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_total_count,
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_draw_count,
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN (CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) END) AS activity_no_draw_count,
    
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN bonus_amount ELSE 0 END) AS activity_total_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount,

    -- ============ 濂栭噾浠诲姟缁熻 ============
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 END) AS bonus_task_draw_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 END) AS bonus_task_no_draw_count,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS bonus_task_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_bonus_amount,

    -- ============ 娓稿浣撻獙閲戠粺璁?============
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN 1 END) AS tourist_draw_count,
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN 1 END) AS tourist_no_draw_count,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS tourist_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS tourist_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY statistic_time, user_id, channel_id, COALESCE(activity_type, '');



-- 鐩爣琛ㄦ彃鍏?娓犻亾缁村害濂栧姳姹囨€昏〃
INSERT INTO today_channel_reward_statistic
SELECT
    -- 1. 鐢ㄦ埛鍘婚噸鎬绘暟
    COUNT(DISTINCT user_id) AS user_count,

    -- 2. VIP绯荤粺缁熻
    COUNT(DISTINCT CASE WHEN reward_scope = 'VIP_REWARD' THEN user_id END) AS vip_user_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN 1 END) AS vip_draw_count,
    COUNT(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN 1 END) AS vip_no_draw_count,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS vip_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS vip_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS vip_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS vip_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'VIP_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS vip_no_draw_bonus_amount,

    -- 3. 杩旀按绯荤粺缁熻
    COUNT(DISTINCT CASE WHEN reward_scope = 'REBATE_REWARD' THEN user_id END) AS rebate_user_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN 1 END) AS rebate_draw_count,
    COUNT(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN 1 END) AS rebate_no_draw_count,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS rebate_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS rebate_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS rebate_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'REBATE_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS rebate_draw_total_withdrawal_amount,

    -- 4. 娲诲姩绯荤粺缁熻 (鍘?Join 鍖栵紝鐩存帴鑱氬悎)
    COUNT(DISTINCT CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN user_id END) AS activity_user_count,
    COUNT(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN 1 END) AS activity_draw_count,
    COUNT(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN 1 END) AS activity_no_draw_count,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' THEN bonus_amount ELSE 0 END) AS activity_total_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'ACTIVITY_REWARD' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount,

    -- 5. 濂栭噾浠诲姟缁熻
    COUNT(DISTINCT CASE WHEN reward_scope = 'BONUS_TASK' THEN user_id END) AS bonus_task_user_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN 1 END) AS bonus_task_draw_count,
    COUNT(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN 1 END) AS bonus_task_no_draw_count,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS bonus_task_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_no_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'BONUS_TASK' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS bonus_task_draw_bonus_amount,

    -- 6. 鏍稿績缁村害 (鏀剧疆鍦ㄧ洰鏍囪〃鎸囧畾浣嶇疆)
    channel_id,
    statistic_time,

    -- 7. 娓稿浣撻獙閲戠粺璁?    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN 1 END) AS tourist_draw_count,
    COUNT(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN 1 END) AS tourist_no_draw_count,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 THEN bonus_amount ELSE 0 END) AS tourist_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_total_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_draw_bonus_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 THEN bonus_amount ELSE 0 END) AS tourist_no_draw_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_withdrawal_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_deposit_amount,
    SUM(CASE WHEN reward_scope = 'TOURIST_TRIAL_BONUS' AND draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS tourist_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND reward_scope IN ('VIP_REWARD', 'REBATE_REWARD', 'ACTIVITY_REWARD', 'BONUS_TASK', 'TOURIST_TRIAL_BONUS')
GROUP BY statistic_time, channel_id;



-- 鐩爣琛ㄦ彃鍏?娓犻亾缁村害娲诲姩姹囨€昏〃
INSERT INTO today_channel_activity_statistic
SELECT
    -- 1. 鐢ㄦ埛缁熻 (鐢卞師 user_stats 鍚堝苟鑰屾潵)
    COUNT(DISTINCT user_id) AS user_count,
    
    -- 2. 鏍稿績缁村害
    channel_id,
    activity_type,
    statistic_time,
    
    -- 3. 鍥哄畾鍗犱綅绗?    0 AS view_activity_count,
    
    -- 4. 娲诲姩娆℃暟鍘婚噸缁熻 (鐢卞師 activity_stats 鍚堝苟鑰屾潵)
    COUNT(DISTINCT CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR)) AS activity_reward_count,
    
    -- 5. 閲戦姹囨€荤粺璁?(鐢卞師 main_stats 鍚堝苟鑰屾潵)
    SUM(bonus_amount) AS activity_total_amount,
    
    -- 6. 娲诲姩鐢ㄦ埛鏁?(閫昏緫涓婄瓑浜?user_count)
    COUNT(DISTINCT user_id) AS activity_user_count,
    
    -- 7. 棰嗙敤涓庢湭棰嗙敤鍘婚噸缁熻
    COUNT(DISTINCT CASE WHEN draw_flag = 1 THEN CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR) END) AS activity_draw_count,
    COUNT(DISTINCT CASE WHEN draw_flag = 0 THEN CAST(activity_type AS VARCHAR) || '|' || CAST(user_id AS VARCHAR) || '|' || CAST(unique_id AS VARCHAR) END) AS activity_no_draw_count,
    
    -- 8. 缁嗗垎鐘舵€侀噾棰濈粺璁?    SUM(CASE WHEN draw_flag = 1 THEN bonus_amount ELSE 0 END) AS activity_draw_amount,
    SUM(CASE WHEN draw_flag = 0 THEN bonus_amount ELSE 0 END) AS activity_no_draw_amount,
    
    -- 9. 缁嗗垎濂栧姳绫诲瀷閲戦缁熻
    SUM(CASE WHEN reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_total_withdrawal_amount,
    SUM(CASE WHEN reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_total_deposit_amount,
    SUM(CASE WHEN reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_total_bonus_amount,
    
    -- 10. 棰嗙敤鐘舵€佷笅鐨勭粏鍒嗗鍔辩被鍨?    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_draw_withdrawal_amount,
    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_draw_deposit_amount,
    SUM(CASE WHEN draw_flag = 1 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_draw_bonus_amount,
    
    -- 11. 鏈鐢ㄧ姸鎬佷笅鐨勭粏鍒嗗鍔辩被鍨?    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'WITHDRAW' THEN bonus_amount ELSE 0 END) AS activity_no_draw_withdrawal_amount,
    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'DEPOSIT' THEN bonus_amount ELSE 0 END) AS activity_no_draw_deposit_amount,
    SUM(CASE WHEN draw_flag = 0 AND reward_type = 'BONUS' THEN bonus_amount ELSE 0 END) AS activity_no_draw_bonus_amount

FROM dwd_reward_record
WHERE update_time IS NOT NULL
  AND activity_type IS NOT NULL
  AND reward_scope = 'ACTIVITY_REWARD'
GROUP BY statistic_time, channel_id, activity_type;






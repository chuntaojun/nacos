/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.distributed.raft;

import java.time.Duration;

/**
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public final class RaftSysConstants {

    // ========= default setting value ========= //

    /**
     * {@link RaftSysConstants#RAFT_ELECTION_TIMEOUT_MS}
     */
    public static final int DEFAULT_ELECTION_TIMEOUT = 5000;

    /**
     * {@link RaftSysConstants#RAFT_SNAPSHOT_INTERVAL_SECS}
     */
    public static final int DEFAULT_RAFT_SNAPSHOT_INTERVAL_SECS = 30 * 60;

    /**
     * {@link RaftSysConstants#RAFT_CLI_SERVICE_THREAD_NUM}
     */
    public static final int DEFAULT_RAFT_CLI_SERVICE_THREAD_NUM = 8;

    /**
     * {@link RaftSysConstants#RAFT_ROUTE_REFRESH_PERIOD_MS}
     */
    public static final long DEFAULT_RAFT_ROUTE_REFRESH_PERIOD = Duration.ofMinutes(3).toMillis();

    /**
     * {@link RaftSysConstants#RAFT_READ_INDEX_TYPE}
     */
    public static final String DEFAULT_READ_INDEX_TYPE = "ReadOnlySafe";

    /**
     * {@link RaftSysConstants#RAFT_RPC_REQUEST_TIMEOUT_MS}
     */
    public static final int DEFAULT_RAFT_RPC_REQUEST_TIMEOUT_MS = 5000;

    /**
     * 节点之间每次文件 RPC (snapshot拷贝）请求的最大大小，默认为 128 K
     */
    public static final int DEFAULT_MAX_BYTE_COUNT_PER_RPC = 128 * 1024;
    /**
     * 从 leader 往 follower 发送的最大日志个数，默认 1024
     */
    public static final int DEFAULT_MAX_ENTRIES_SIZE = 1024;
    /**
     * 从 leader 往 follower 发送日志的最大 body 大小，默认 512K
     */
    public static final int DEFAULT_MAX_BODY_SIZE = 512 * 1024;
    /**
     * 日志存储缓冲区最大大小，默认256K
     */
    public static final int DEFAULT_MAX_APPEND_BUFFER_SIZE = 256 * 1024;
    /**
     * 选举定时器间隔会在指定时间之外随机的最大范围，默认1秒
     */
    public static final int DEFAULT_MAX_ELECTION_DELAY_MS = 1000;
    /**
     * 指定选举超时时间和心跳间隔时间之间的比值。心跳间隔等于
     * electionTimeoutMs/electionHeartbeatFactor，默认10分之一。
     */
    public static final int DEFAULT_ELECTION_HEARTBEAT_FACTOR = 10;
    /**
     * 向 leader 提交的任务累积一个批次刷入日志存储的最大批次大小，默认 32 个任务
     */
    public static final int DEFAULT_APPLY_BATCH = 32;
    /**
     * 写入日志、元信息的时候必要的时候调用 fsync，通常都应该为 true
     */
    public static final boolean DEFAULT_SYNC = true;
    /**
     * 写入 snapshot/raft 元信息是否调用 fsync，默认为 false，
     * 在 sync 为 true 的情况下，优选尊重 sync
     */
    public static final boolean DEFAULT_SYNC_META = false;
    /**
     * 内部 disruptor buffer 大小，如果是写入吞吐量较高的应用，需要适当调高该值，默认 16384
     */
    public static final int DEFAULT_DISRUPTOR_BUFFER_SIZE = 16384;
    /**
     * 是否启用复制的 pipeline 请求优化，默认打开
     */
    public static final boolean DEFAULT_REPLICATOR_PIPELINE = true;
    /**
     * 在启用 pipeline 请求情况下，最大 in-flight 请求数，默认256
     */
    public static final int DEFAULT_MAX_REPLICATOR_INFLIGHT_MSGS = 256;
    /**
     * 是否启用 LogEntry checksum
     */
    public static final boolean DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM = false;

    // ========= setting key ========= //

    /**
     * Raft port
     */
    public static final String RAFT_PORT = "raft_port";

    /**
     * Election timeout in milliseconds
     */
    public static final String RAFT_ELECTION_TIMEOUT_MS = "raft_election_timeout_ms";

    /**
     * Snapshot interval in seconds
     */
    public static final String RAFT_SNAPSHOT_INTERVAL_SECS = "raft_snapshot_interval_secs";

    /**
     * Requested retries
     */
    public static final String REQUEST_FAILOVER_RETRIES = "raft_request_failoverRetries";

    /**
     * raft internal worker threads
     */
    public static final String RAFT_CORE_THREAD_NUM = "raft_core_thread_num";

    /**
     * Number of threads required for raft business request processing
     */
    public static final String RAFT_CLI_SERVICE_THREAD_NUM = "raft_cli_service_thread_num";

    /**
     * raft group routing table information refresh interval
     */
    public static final String RAFT_ROUTE_REFRESH_PERIOD_MS = "raft_route_refresh_period_ms";

    /**
     * raft linear read strategy, defaults to index
     */
    public static final String RAFT_READ_INDEX_TYPE = "raft_read_index_type";

    /**
     * rpc request timeout, default 5 seconds
     */
    public static final String RAFT_RPC_REQUEST_TIMEOUT_MS = "raft_rpc_request_timeout_ms";

    /**
     * Maximum size of each file RPC (snapshot copy) request between nodes, default is 128 K
     */
    public static final String MAX_BYTE_COUNT_PER_RPC = "raft_max_byte_count_per_rpc";

    /**
     * Maximum number of logs sent from leader to follower, default is 1024
     */
    public static final String MAX_ENTRIES_SIZE = "raft_max_entries_size";

    /**
     * Maximum body size for sending logs from leader to follower, default is 512K
     */
    public static final String MAX_BODY_SIZE = "raft_max_body_size";

    /**
     * Maximum log storage buffer size, default 256K
     */
    public static final String MAX_APPEND_BUFFER_SIZE = "raft_max_append_buffer_size";

    /**
     * Election timer interval will be a random maximum outside the specified time, default is 1 second
     */
    public static final String MAX_ELECTION_DELAY_MS = "raft_max_election_delay_ms";

    /**
     * Specify the ratio between election timeout and heartbeat interval. Heartbeat interval is equal to
     * electionTimeoutMs/electionHeartbeatFactor，One tenth by default.
     */
    public static final String ELECTION_HEARTBEAT_FACTOR = "raft_election_heartbeat_factor";

    /**
     * The tasks submitted to the leader accumulate the maximum batch
     * size of a batch flush log storage. The default is 32 tasks.
     */
    public static final String APPLY_BATCH = "raft_apply_batch";

    /**
     * Call fsync when necessary when writing logs and meta information, usually should be true
     */
    public static final String SYNC = "raft_sync";

    /**
     * Whether to write snapshot / raft meta-information to call fsync.
     * The default is false. When sync is true, it is preferred to respect sync.
     */
    public static final String SYNC_META = "raft_sync_meta";

    /**
     * Internal disruptor buffer size. For applications with high write throughput,
     * you need to increase this value. The default value is 16384.
     */
    public static final String DISRUPTOR_BUFFER_SIZE = "raft_disruptor_buffer_size";

    /**
     * Whether to enable replication of pipeline request optimization, which is enabled by default
     */
    public static final String REPLICATOR_PIPELINE = "raft_replicator_pipeline";

    /**
     * Maximum number of in-flight requests with pipeline requests enabled, default is 256
     */
    public static final String MAX_REPLICATOR_INFLIGHT_MSGS = "raft_max_replicator_inflight_msgs";

    /**
     * Whether to enable LogEntry checksum
     */
    public static final String ENABLE_LOG_ENTRY_CHECKSUM = "raft_enable_log_entry_checksum";
}

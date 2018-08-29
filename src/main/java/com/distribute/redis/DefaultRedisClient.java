package com.distribute.redis;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.api.StatefulConnection;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author jibingkun
 * @date 2018/8/23.
 */
public abstract class DefaultRedisClient {

    private Logger LOG = LoggerFactory.getLogger(DefaultRedisClient.class);

    private volatile boolean closing = false;

    // 是否是集群
    private boolean isCluster = false;

    // 集群客户端
    private RedisClusterClient clusterClient;

    // 单机客户端
    private RedisClient client;

    private GenericObjectPool<StatefulConnection<String, String>> pool;

    private ThreadLocal<RedisCommands<String, String>> transactionCmd = new ThreadLocal<>();

    public DefaultRedisClient() {

    }

    public DefaultRedisClient(String uri) {
        this(uri, false);
    }

    public DefaultRedisClient(String uri, boolean readFromMaster) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxWaitMillis(300);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        this.init(uri, poolConfig, readFromMaster);
    }

    public DefaultRedisClient(String uri, GenericObjectPoolConfig poolConfig) {
        this.init(uri, poolConfig, false);
    }

    public DefaultRedisClient(String uri, GenericObjectPoolConfig poolConfig, boolean readFromMaster) {
        this.init(uri, poolConfig, readFromMaster);
    }

    /**
     * 初始化
     */
    private void init(String uri, GenericObjectPoolConfig poolConfig, boolean readFromMaster) {

        // 配置集群选项
        ClusterClientOptions options = ClusterClientOptions.builder()
                // 当连接中断的时候是否重新连接
                .autoReconnect(true)
                // 在连接之前ping测试
                .pingBeforeActivateConnection(true)
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(2, TimeUnit.SECONDS)
                        .keepAlive(true)
                        .tcpNoDelay(true).build()
                ).build();

        // 判断服务使用的是集群还是单机版本
        if (uri.contains(",")) {
            this.isCluster = true;
            this.clusterClient = RedisClusterClient.create(uri);
            this.clusterClient.setOptions(options);
        } else {
            this.isCluster = false;
            this.client = RedisClient.create(uri);
            this.client.setOptions(options);
        }

        ConnectionPoolSupport.createGenericObjectPool(
                this.isCluster ? () -> {
                    StatefulRedisClusterConnection<String, String> conn = clusterClient.connect();
                    if (readFromMaster) {
                        conn.setReadFrom(ReadFrom.MASTER);
                    } else {
                        conn.setReadFrom(ReadFrom.SLAVE_PREFERRED);
                    }
                    return conn;
                } : () -> client.connect(), poolConfig);
    }

    /**
     * 关闭
     */
    public void close() {
        this.closing = true;
        this.pool.close();
        if (this.isCluster) {
            this.clusterClient.shutdown();
        } else {
            this.client.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    protected <T, R> R doCmd(Function<T, R> cmd) {
        RedisCommands<String, String> txCmd = this.transactionCmd.get();
        if (txCmd != null) {
            return cmd.apply((T) txCmd);
        } else {
            try (StatefulConnection<String, String> connection = this.pool.borrowObject()) {
                Object redisCmd = this.isCluster ? ((StatefulRedisClusterConnection<String, String>) connection).sync()
                        : ((StatefulRedisConnection<String, String>) connection).sync();
                return cmd.apply((T) redisCmd);
            } catch (Exception e) {
                throw new RuntimeException("failed to obtain redis connection", e);
            }
        }
    }
}

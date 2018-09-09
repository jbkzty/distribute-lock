package com.distribute.redis;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.SetArgs;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.api.StatefulConnection;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.api.sync.RedisListCommands;
import com.lambdaworks.redis.api.sync.RedisStringCommands;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.SlotHash;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author jibingkun
 * @date 2018/8/23.
 */
public class DefaultRedisClient {

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
     * https://lettuce.io/lettuce-4/4.4.6.Final/api/com/lambdaworks/redis/cluster/api/StatefulRedisClusterConnection.html
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

        this.pool = ConnectionPoolSupport.createGenericObjectPool(
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

    /**
     * 这样的做法是由于使用Jedis，针对不同实体需要定义不同的template实例带来的困扰
     * 统一封装了接口：RedisClusterCommands
     *
     * @param cmd
     * @param <T>
     * @param <R>
     * @return
     */
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


    /**
     * string
     * https://lettuce.io/lettuce-4/4.4.0.Final/api/com/lambdaworks/redis/api/sync/RedisStringCommands.html
     */
    private <R> R doStringCmd(Function<RedisStringCommands<String, String>, R> stringCmd) {
        return this.doCmd(stringCmd);
    }

    /**
     * list
     * https://lettuce.io/lettuce-4/4.4.6.Final/api/com/lambdaworks/redis/api/sync/RedisListCommands.html
     */
    private <R> R doListCmd(Function<RedisListCommands<String, String>, R> listCmd) {
        return this.doCmd(listCmd);
    }

    /**
     * Get the value of a key
     */
    public String get(String key) {
        return this.doStringCmd((cmd) -> cmd.get(key));
    }

    /**
     * Returns the bit value at offset in the string value stored at key.
     */
    public Long getBit(String key, long offset) {
        return this.doStringCmd((cmd) -> cmd.getbit(key, offset));
    }

    /**
     * Get a substring of the string stored at a key.
     */
    public String getRange(String key, long start, long end) {
        return this.doStringCmd((cmd) -> cmd.getrange(key, start, end));
    }

    /**
     * Set the string value of a key and return its old value.
     */
    public String getset(String key, String value) {
        return this.doStringCmd((cmd) -> cmd.getset(key, value));
    }

    /**
     * Increment the integer value of a key by one.
     */
    public Long incr(String key) {
        return this.doStringCmd((cmd) -> cmd.incr(key));
    }

    /**
     * Increment the integer value of a key by the given amount.
     */
    public Long incrby(String key, long amount) {
        return this.doStringCmd((cmd) -> cmd.incrby(key, amount));
    }

    /**
     * Increment the float value of a key by the given amount.
     */
    public Double incrbyfloat(String key, double amount) {
        return this.doStringCmd((cmd) -> cmd.incrbyfloat(key, amount));
    }

    /**
     * Get the values of all the given keys.
     */
    public List<String> mget(String... keys) {
        return this.doStringCmd((cmd) -> cmd.mget(keys));
    }

    /**
     * Set multiple keys to multiple values.
     */
    public String mset(Map<String, String> kv) {
        return this.doStringCmd((cmd) -> cmd.mset(kv));
    }

    /**
     * Set multiple keys to multiple values, only if none of the keys exist.
     */
    public Boolean msetnx(Map<String, String> map) {
        if (map.size() == 0) {
            return true;
        }

        Integer keySlot = null;
        for (String key : map.keySet()) {
            int slot = SlotHash.getSlot(key);
            if (keySlot == null) {
                keySlot = slot;
            } else if (slot != keySlot) {
                return false;
            }
        }
        return this.doStringCmd((cmd) -> cmd.msetnx(map));
    }

    /**
     * Set the string value of a key.
     */
    public String set(String key, String value) {
        return this.doStringCmd((cmd) -> cmd.set(key, value));
    }

    /**
     * Set the string value of a key.
     */
    public String set(String key, String value, Long ex, Long px, Boolean nx, Boolean xx) {
        SetArgs args = new SetArgs();
        if (ex != null) {
            args.ex(ex);
        }
        if (px != null) {
            args.px(px);
        }
        if (nx) {
            args.nx();
        }
        if (xx) {
            args.xx();
        }
        return this.doStringCmd((cmd) -> cmd.set(key, value, args));
    }

    /**
     * Sets or clears the bit at offset in the string value stored at key.
     */
    public Long setbit(String key, long offset, int value) {
        return this.doStringCmd((cmd) -> cmd.setbit(key, offset, value));
    }

    /**
     * Set the value and expiration of a key.
     */
    public String setex(String key, long seconds, String value) {
        return this.doStringCmd((cmd) -> cmd.setex(key, seconds, value));
    }

    /**
     * Set the value and expiration in milliseconds of a key.
     */
    public String psetex(String key, long milliseconds, String value) {
        return this.doStringCmd((cmd) -> cmd.psetex(key, milliseconds, value));
    }

    /**
     * Set the value of a key, only if the key does not exist.
     */
    public Boolean setnx(String key, String value) {
        return this.doStringCmd((cmd) -> cmd.setnx(key, value));
    }

    /**
     * Overwrite part of a string at key starting at the specified offset.
     */
    public Long setrange(String key, long offset, String value) {
        return this.doStringCmd((cmd) -> cmd.setrange(key, offset, value));
    }

    /**
     * Get the length of the value stored in a key.
     */
    public Long strlen(String key) {
        return this.doStringCmd((cmd) -> cmd.strlen(key));
    }

    public String lindex(String key, long index) {
        return this.doListCmd((cmd) -> cmd.lindex(key, index));
    }

    public Long linsert(String key, boolean before, String pivot, String value) {
        return this.doListCmd((cmd) -> cmd.linsert(key, before, pivot, value));
    }

    public Long llen(String key) {
        return this.doListCmd((cmd) -> cmd.llen(key));
    }

    /**
     * Remove and get the first element in a list.
     */
    public String lpop(String key) {
        return this.doListCmd((cmd) -> cmd.lpop(key));
    }

    /**
     * Prepend one or multiple values to a list.
     */
    public Long lpush(String key, String... values) {
        return this.doListCmd((cmd) -> cmd.lpush(key, values));
    }

    /**
     * Prepend values to a list, only if the list exists.
     */
    public Long lpushx(String key, String... values) {
        return this.doListCmd((cmd) -> cmd.lpushx(key, values));
    }

    public List<String> lrange(String key, long start, long stop) {
        return this.doListCmd((cmd) -> cmd.lrange(key, start, stop));
    }

    public Long lrem(String key, long count, String value) {
        return this.doListCmd((cmd) -> cmd.lrem(key, count, value));
    }

    public String lset(String key, long index, String value) {
        return this.doListCmd((cmd) -> cmd.lset(key, index, value));
    }

    public String ltrim(String key, long start, long stop) {
        return this.doListCmd((cmd) -> cmd.ltrim(key, start, stop));
    }

    public String rpop(String key) {
        return this.doListCmd((cmd) -> cmd.rpop(key));
    }

    /**
     * Append one or multiple values to a list.
     */
    public Long rpush(String key, String... values) {
        return this.doListCmd((cmd) -> cmd.rpush(key, values));
    }

    public Long rpushx(String key, String... values) {
        return this.doListCmd((cmd) -> cmd.rpushx(key, values));
    }
}

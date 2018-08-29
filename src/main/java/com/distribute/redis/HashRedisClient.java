package com.distribute.redis;

import com.lambdaworks.redis.api.sync.RedisHashCommands;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * https://lettuce.io/lettuce-4/4.4.0.Final/api/
 *
 * @author jibingkun
 * @date 2018/8/27.
 */
public class HashRedisClient extends DefaultRedisClient {

    /**
     * hash
     */
    private <R> R doHashCmd(Function<RedisHashCommands<String, String>, R> hashCmd) {
        return super.doCmd(hashCmd);
    }

    /**
     * Delete one or more hash fields.
     */
    public Long hdel(String key, String... fields) {
        return this.doHashCmd((cmd) -> cmd.hdel(key, fields));
    }

    /**
     * Determine if a hash field exists.
     */
    public Boolean hexists(String key, String field) {
        return this.doHashCmd((cmd) -> cmd.hexists(key, field));
    }

    /**
     * Get the value of a hash field.
     */
    public String hget(String key, String field) {
        return this.doHashCmd((cmd) -> cmd.hget(key, field));
    }

    /**
     * Increment the integer value of a hash field by the given number.
     */
    public Long hincrby(String key, String field, long amount) {
        return this.doHashCmd((cmd) -> cmd.hincrby(key, field, amount));
    }

    /**
     * Increment the float value of a hash field by the given amount.
     */
    public Double hincrbyfloat(String key, String field, double amount) {
        return this.doHashCmd((cmd) -> cmd.hincrbyfloat(key, field, amount));
    }

    /**
     * Get all the fields and values in a hash.
     */
    public Map<String, String> hgetall(String key) {
        return this.doHashCmd((cmd) -> cmd.hgetall(key));
    }

    /**
     * Get all the fields in a hash.
     */
    public List<String> hkeys(String key) {
        return this.doHashCmd((cmd) -> cmd.hkeys(key));
    }

    /**
     * Get the number of fields in a hash.
     */
    public Long hlen(String key) {
        return this.doHashCmd((cmd) -> cmd.hlen(key));
    }

    /**
     * Get the values of all the given hash fields.
     */
    public List<String> hmget(String key, String... fields) {
        return this.doHashCmd((cmd) -> cmd.hmget(key, fields));
    }

    /**
     * Set multiple hash fields to multiple values.
     */
    public String hmset(String key, Map<String, String> map) {
        return this.doHashCmd((cmd) -> cmd.hmset(key, map));
    }

    /**
     * Set the string value of a hash field.
     */
    public Boolean hset(String key, String field, String value) {
        return this.doHashCmd((cmd) -> cmd.hset(key, field, value));
    }

    /**
     * Set the value of a hash field, only if the field does not exist.
     */
    public Boolean hsetnx(String key, String field, String value) {
        return this.doHashCmd((cmd) -> cmd.hsetnx(key, field, value));
    }

    /**
     * Get the string length of the field value in a hash.
     */
    public Long hstrlen(String key, String field) {
        return this.doHashCmd((cmd) -> cmd.hstrlen(key, field));
    }

    /**
     * Get all the values in a hash.
     */
    public List<String> hvals(String key) {
        return this.doHashCmd((cmd) -> cmd.hvals(key));
    }
}

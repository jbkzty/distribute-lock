package com.distribute.redis;

import com.lambdaworks.redis.api.sync.RedisKeyCommands;

import java.util.Date;
import java.util.function.Function;

/**
 * @author jibingkun
 * @date 2018/8/27.
 */
public class KeyRedisClient extends DefaultRedisClient {

    /**
     * key
     */
    private <R> R doKeyCmd(Function<RedisKeyCommands<String, String>, R> keyCmd) {
        return super.doCmd(keyCmd);
    }

    /**
     * Delete one or more keys.
     */
    public Long del(String... keys) {
        return this.doKeyCmd((cmd) -> cmd.del(keys));
    }

    /**
     * Determine how many keys exist.
     */
    public Long exists(String... keys) {
        return this.doKeyCmd((cmd) -> cmd.exists(keys));
    }

    /**
     * Set a key's time to live in seconds.
     */
    public Boolean expire(String key, int seconds) {
        return this.doKeyCmd((cmd) -> cmd.expire(key, seconds));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp.
     */
    public Boolean expireat(String key, Date timestamp) {
        return this.doKeyCmd((cmd) -> cmd.expireat(key, timestamp));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp.
     */
    public Boolean expireat(String key, int timestamp) {
        return this.doKeyCmd((cmd) -> cmd.expireat(key, timestamp));
    }

    public Boolean pexpire(String key, long milliseconds) {
        return this.doKeyCmd((cmd) -> cmd.pexpire(key, milliseconds));
    }

    /**
     * Set a key's time to live in milliseconds.
     */
    public Boolean pexpireat(String key, long timestamp) {
        return this.doKeyCmd((cmd) -> cmd.pexpireat(key, timestamp));
    }

    /**
     * Get the time to live for a key in milliseconds.
     */
    public Long pttl(String key) {
        return this.doKeyCmd((cmd) -> cmd.pttl(key));
    }

    /**
     * Get the time to live for a key.
     */
    public Long ttl(String key) {
        return this.doKeyCmd((cmd) -> cmd.ttl(key));
    }

    /**
     * Determine the type stored at key.
     */
    public String type(String key) {
        return this.doKeyCmd((cmd) -> cmd.type(key));
    }
}

package com.distribute.redis;

import com.lambdaworks.redis.SetArgs;
import com.lambdaworks.redis.api.sync.RedisStringCommands;
import com.lambdaworks.redis.cluster.SlotHash;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 字符串的相关操作
 * <p>
 * https://lettuce.io/lettuce-4/4.4.0.Final/api/com/lambdaworks/redis/api/sync/RedisStringCommands.html
 *
 * @author jibingkun
 * @date 2018/8/27.
 */
public class StringRedisClient extends DefaultRedisClient {

    /**
     * string
     */
    private <R> R doStringCmd(Function<RedisStringCommands<String, String>, R> stringCmd) {
        return super.doCmd(stringCmd);
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
}

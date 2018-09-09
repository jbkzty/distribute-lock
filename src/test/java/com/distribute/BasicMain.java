package com.distribute;

import com.distribute.redis.DefaultRedisClient;

/**
 * @author jibingkun
 * @date 2018/8/29.
 */
public class BasicMain {

    public static void main(String[] args) {

        String redisUrl = "redis://192.168.1.250:6470,192.168.1.250:6471";
        DefaultRedisClient redisClient = new DefaultRedisClient(redisUrl);

        System.out.println("test redis set and get start");
        redisClient.set("key-abc", "value-abc");
        String value1 = redisClient.get("key-abc");
        System.out.println("----" + value1);

        System.out.println("test redis getRange start");
        String value2 = redisClient.getRange("key-abc", 1, 2);
        System.out.println("----" + value2);

        System.out.println("test redis getset start");
        String value3 = redisClient.getset("key-abc", "value-abcd");
        String value4 = redisClient.get("key-abc");
        System.out.println("----" + value3);
        System.out.println("----" + value4);

        System.out.println("test redis list start");
        redisClient.rpush("list-redis","a");
        redisClient.rpush("list-redis","b");
        redisClient.rpush("list-redis","c");
        long listSize = redisClient.llen("list-redis");
        System.out.println("----" + listSize);

    }
}

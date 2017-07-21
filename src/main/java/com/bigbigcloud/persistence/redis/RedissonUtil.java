package com.bigbigcloud.persistence.redis;

import com.bigbigcloud.BrokerConstants;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017/5/23.
 */


public final class RedissonUtil {
  //全局变量，初始化后即获取client
    private static final RedissonUtil redisUtils = new RedissonUtil();

    private RedissonClient redissonClient;

    private RedissonUtil() {
        Config config = new Config();
        try {
            config = Config.fromJSON(new File(BrokerConstants.CONFIG_LOCATION + BrokerConstants.REDIS_CONFIG));
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.useSingleServer();
        this.redissonClient = Redisson.create(config);
    }

    public static RedissonClient getRedisson() {
        return redisUtils.redissonClient;
    }

}

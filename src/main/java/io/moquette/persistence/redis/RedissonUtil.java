package io.moquette.persistence.redis;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReplicatedServersConfig;

/**
 * Created by Administrator on 2017/5/23.
 */


public final class RedissonUtil {

    //Redis服务器IP
    private static String HOST = "";
    private static RedissonUtil redisUtils;
    //Redis的端口号
    private static int PORT = 6379;

    public static RedissonUtil getInstance() {
        if (redisUtils == null)
            synchronized (RedissonUtil.class) {
                if (redisUtils == null)
                    redisUtils = new RedissonUtil();
            }
        return redisUtils;
    }


    /**
     * 使用config创建Redisson
     * Redisson是用于连接Redis Server的基础类
     *
     * @param config
     * @return
     */
    public RedissonClient getRedisson(Config config) {
        RedissonClient redisson = Redisson.create(config);
        System.out.println("成功连接Redis Server");
        return redisson;
    }

    /**
     * 使用ip地址和端口创建Redisson
     *
     * @param ip
     * @param port
     * @return
     */
    public RedissonClient getRedisson(String ip, String port, String password) {
        Config config = new Config();
//        config.useReplicatedServers().addNodeAddress(ip + ":" + port).setPassword(password);
//        config.useClusterServers().addNodeAddress(ip + ":" + port).setPassword(password);
        config.useSingleServer().setAddress(ip + ":" + port).setPassword(password).setConnectionPoolSize(8).setConnectionMinimumIdleSize(8);
        RedissonClient redisson = Redisson.create(config);
        System.out.println("成功连接Redis Server" + "\t" + "连接" + ip + ":" + port + "服务器");
        return redisson;
    }

    /**
     * 关闭Redisson客户端连接
     *
     * @param redisson
     */
    public static void closeRedisson(RedissonClient redisson) {
        redisson.shutdown();
        System.out.println("成功关闭Redis Client连接");
    }

}

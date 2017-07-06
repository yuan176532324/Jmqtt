/*
 * Copyright (c) 2012-2017 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package com.bigibgcloud.spi.impl.security;

import com.bigibgcloud.persistence.redis.RedissonUtil;
import com.bigibgcloud.spi.security.IAuthenticator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public class BBCAuthenticator implements IAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(BBCAuthenticator.class);
    private ConcurrentMap<String, String> passwordStore;
    private static final Producer<String, UserInfo> producer;

    static {
        Properties producerProperties = new Properties();
        InputStream in = ClassLoader.getSystemResourceAsStream("kafkaProducer4UI.properties");
        try {
            producerProperties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, UserInfo>(producerProperties);
    }

    public BBCAuthenticator() {
        RedissonClient redissonClient = RedissonUtil.getRedisson();
        passwordStore = redissonClient.getMap("passwordStore");
    }

    public boolean checkValid(String clientId, String username, byte[] password) {
        LOG.info("begin to check password!,clientId:" + clientId);
        String DEVICE = "device:";
        String APP = "app:";
        if (passwordStore.containsKey(clientId)) {
            LOG.info("get userinfo from redis,clientId:" + clientId);
            return passwordStore.get(clientId).equals(new String(password));
        } else {
            LOG.info("put userinfo into kafka!,clientId:" + clientId);
            if (clientId.contains(DEVICE) || clientId.contains(APP)) {
                String[] strs = clientId.split(":");
                if (strs.length > 3) {
                    UserInfo userInfo = new UserInfo();
                    userInfo.setFlag(true);
                    userInfo.setClientId(clientId);
                    userInfo.setKey(strs[1]);
                    ProducerRecord<String, UserInfo> record = new ProducerRecord<String, UserInfo>("BBCMTUI", userInfo);
                    producer.send(record);
                    return true;
                }
            }
        }
        return false;
    }
}

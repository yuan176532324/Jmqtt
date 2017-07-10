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

package com.bigbigcloud.spi.impl.security;

import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.spi.security.IAuthenticator;
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
    public BBCAuthenticator() {
        RedissonClient redissonClient = RedissonUtil.getRedisson();
        passwordStore = redissonClient.getMap("passwordStore");
    }

    public boolean checkValid(String clientId, String username, byte[] password) {
        return true;
    }
}

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

package io.moquette.persistence;

import io.moquette.persistence.redis.RedisMessagesStore;
import io.moquette.persistence.redis.RedisSessionsStore;
import io.moquette.persistence.redis.RedisUtil;
import io.moquette.persistence.redis.RedissonUtil;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.IStore;
import org.redisson.api.RedissonClient;

public class RedisStorageService implements IStore {

    private RedisSessionsStore m_sessionsStore;
    private RedisMessagesStore m_messagesStore;

    public RedisStorageService() {
        RedissonClient redis = RedissonUtil.getInstance().getRedisson("r-wz99dc86eb498824.redis.rds.aliyuncs.com","6379","ALLwinner123");
        m_messagesStore = new RedisMessagesStore(redis);
        m_sessionsStore = new RedisSessionsStore(redis, m_messagesStore);
        m_messagesStore.initStore();
        m_sessionsStore.initStore();
    }

    @Override
    public IMessagesStore messagesStore() {
        return m_messagesStore;
    }

    @Override
    public ISessionsStore sessionsStore() {
        return m_sessionsStore;
    }

    @Override
    public void close() {
    }
}

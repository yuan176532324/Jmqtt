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

package com.bigbigcloud.persistence;

import com.bigbigcloud.persistence.redis.RedisMessagesStore;
import com.bigbigcloud.persistence.redis.RedisSessionsStore;
import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.spi.IMessagesStore;
import com.bigbigcloud.spi.ISessionsStore;
import com.bigbigcloud.spi.IStore;
import org.redisson.api.RedissonClient;

import java.io.IOException;

public class RedisStorageService implements IStore {

    private RedisSessionsStore m_sessionsStore;
    private RedisMessagesStore m_messagesStore;
    private RedissonClient redis = RedissonUtil.getRedisson();

    public RedisStorageService() throws IOException {
        m_messagesStore = new RedisMessagesStore(redis);
        m_sessionsStore = new RedisSessionsStore(redis);
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
        RedissonUtil.closeRedisson(redis);
    }
}

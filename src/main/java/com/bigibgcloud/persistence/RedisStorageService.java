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

package com.bigibgcloud.persistence;

import com.bigibgcloud.persistence.redis.RedisMessagesStore;
import com.bigibgcloud.persistence.redis.RedisSessionsStore;
import com.bigibgcloud.persistence.redis.RedissonUtil;
import com.bigibgcloud.spi.IMessagesStore;
import com.bigibgcloud.spi.ISessionsStore;
import com.bigibgcloud.spi.IStore;
import io.moquette.persistence.redis.*;
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

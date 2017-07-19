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

package com.bigbigcloud.persistence.redis;

import com.bigbigcloud.common.model.StoredMessage;
import com.bigbigcloud.spi.IMessagesStore;
import com.bigbigcloud.spi.IMatchingCondition;
import com.bigbigcloud.spi.impl.subscriptions.Topic;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import static com.bigbigcloud.BrokerConstants.RETAINED_STORE;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * IMessagesStore implementation backed by MapDB.
 */
public class RedisMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisMessagesStore.class);

    private RedissonClient redis;

    private RBucket<StoredMessage> storedMessageRBucket;

    public RedisMessagesStore(RedissonClient redis) {
        this.redis = redis;
    }

    @Override
    public void initStore() {
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("Scanning retained messages");
        List<StoredMessage> results = new ArrayList<>();
        Collection<String> keyForTopic = redis.getKeys().findKeysByPattern(RETAINED_STORE);
        for (String key : keyForTopic) {
            storedMessageRBucket = redis.getBucket(key);
            String topic = key.replace(RETAINED_STORE, "");
            StoredMessage storedMsg = storedMessageRBucket.get();
            if (condition.match(new Topic(topic))) {
                results.add(storedMsg);
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Retained messages have been scanned matchingMessages={}", results);
        }

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        LOG.debug("Cleaning retained messages. Topic={}", topic);
        storedMessageRBucket = redis.getBucket(RETAINED_STORE + topic);
        storedMessageRBucket.delete();
    }

    @Override
    public void storeRetained(Topic topic, StoredMessage storedMessage) {
        LOG.debug("Store retained message for topic={}, CId={}", topic, storedMessage.getClientID());
        if (storedMessage.getClientID() == null) {
            throw new IllegalArgumentException("Message to be persisted must have a not null client ID");
        }
        storedMessageRBucket = redis.getBucket(RETAINED_STORE + topic);
        storedMessageRBucket.set(storedMessage);
        storedMessageRBucket.expire(7, DAYS);
    }
}

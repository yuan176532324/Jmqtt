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

package com.bigbigcloud.spi.impl;

import com.bigbigcloud.common.model.MessageGUID;
import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.persistence.redis.TrackedMessage;
import com.bigbigcloud.server.ConnectionDescriptorStore;
import com.bigbigcloud.spi.ClientSession;
import com.bigbigcloud.spi.ISessionsStore;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.redisson.api.RBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import static com.bigbigcloud.BrokerConstants.MESSAGE_STATUS;
import static com.bigbigcloud.persistence.redis.MessageStatus.PUB_OFFLINE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

class PersistentQueueMessageSender {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentQueueMessageSender.class);
    private final ConnectionDescriptorStore connectionDescriptorStore;
    private ISessionsStore r_sessionsStore;

    PersistentQueueMessageSender(ConnectionDescriptorStore connectionDescriptorStore) {
        this.connectionDescriptorStore = connectionDescriptorStore;
    }

    void sendPublish(ClientSession clientsession, MqttPublishMessage pubMessage, MessageGUID messageGUID) {
        String clientId = clientsession.clientID;
        final int messageId = pubMessage.variableHeader().messageId();
        final String topicName = pubMessage.variableHeader().topicName();
        MqttQoS qos = pubMessage.fixedHeader().qosLevel();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending PUBLISH message. MessageId={}, CId={}, topic={}, qos={}, payload={}", messageId,
                    clientId, topicName, qos, DebugUtils.payload2Str(pubMessage.payload()));
        } else {
            LOG.info("Sending PUBLISH message. MessageId={}, CId={}, topic={}", messageId, clientId, topicName);
        }

        RBucket<TrackedMessage> rBucket = RedissonUtil.getRedisson().getBucket(MESSAGE_STATUS + clientId + "_" + messageGUID.toString());

        boolean messageDelivered = connectionDescriptorStore.sendMessage(pubMessage, messageId, clientId, messageGUID);

        if (!messageDelivered) {
            if (qos != AT_MOST_ONCE && !clientsession.isCleanSession()) {
                LOG.warn("PUBLISH message could not be delivered. It will be stored. MessageId={}, CId={}, topic={}, "
                        + "qos={}, cleanSession={}", messageId, clientId, topicName, qos, false);
                rBucket.set(new TrackedMessage(PUB_OFFLINE), 7, TimeUnit.DAYS);
                clientsession.enqueue(r_sessionsStore.inFlightAck(clientId, messageId));
            } else {
                LOG.warn("PUBLISH message could not be delivered. It will be discarded. MessageId={}, CId={}, topic={}, " +
                        "qos={}, cleanSession={}", messageId, clientId, topicName, qos, true);
            }
        }
    }
}
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
import com.bigbigcloud.common.model.StoredMessage;
import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.persistence.redis.TrackedMessage;
import com.bigbigcloud.spi.ISessionsStore;
import com.bigbigcloud.spi.ClientSession;
import com.bigbigcloud.spi.impl.subscriptions.SubscriptionsDirectory;
import com.bigbigcloud.server.ConnectionDescriptorStore;
import com.bigbigcloud.spi.impl.subscriptions.Subscription;
import com.bigbigcloud.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.redisson.api.RBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.bigbigcloud.BrokerConstants.MESSAGE_STATUS;
import static com.bigbigcloud.BrokerConstants.OFFLINE_MESSAGES;
import static com.bigbigcloud.persistence.redis.MessageStatus.*;

class MessagesPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(MessagesPublisher.class);
    private final ConnectionDescriptorStore connectionDescriptors;
    private final ISessionsStore m_sessionsStore;
    private final PersistentQueueMessageSender messageSender;
    private final SubscriptionsDirectory subscriptions;

    public MessagesPublisher(ConnectionDescriptorStore connectionDescriptors, ISessionsStore sessionsStore,
                             PersistentQueueMessageSender messageSender, SubscriptionsDirectory subscriptions) {
        this.connectionDescriptors = connectionDescriptors;
        this.m_sessionsStore = sessionsStore;
        this.messageSender = messageSender;
        this.subscriptions = subscriptions;
    }

    private static MqttPublishMessage notRetainedPublish(String topic, MqttQoS qos, ByteBuf message) {
        return notRetainedPublishWithMessageId(topic, qos, message, 0);
    }

    private static MqttPublishMessage notRetainedPublishWithMessageId(String topic, MqttQoS qos, ByteBuf message,
                                                                      int messageId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, messageId);
        return new MqttPublishMessage(fixedHeader, varHeader, message);
    }

    void publish2Subscribers(StoredMessage pubMsg, Topic topic) throws IOException {
        List<Subscription> topicMatchingSubscriptions = subscriptions.matches(topic);
        final String topic1 = pubMsg.getTopic();
        final MqttQoS publishingQos = pubMsg.getQos();
        final ByteBuf origPayload = pubMsg.getPayload();
        //标记 发布者+消息 状态为READY_TO_PUB
        if (pubMsg.getGuid() != null) {
            RBucket<TrackedMessage> rBucket_pub = RedissonUtil.getRedisson().getBucket(MESSAGE_STATUS + pubMsg.getClientID() + "_" + pubMsg.getMessageId() + "_" + pubMsg.getGuid().toString());
            rBucket_pub.set(new TrackedMessage(READY_TO_PUB), 1, TimeUnit.DAYS);
        }
        //为订阅者分发消息
        for (final Subscription sub : topicMatchingSubscriptions) {
            if (pubMsg.getGuid() == null) {
                pubMsg.setGuid(new MessageGUID(UUID.randomUUID().toString()));
            }
            LOG.info("pub msg clientid is :{} ,guid is:{}", sub.getClientId(), pubMsg.getGuid());
            RBucket<TrackedMessage> rBucket_sub = RedissonUtil.getRedisson().getBucket(MESSAGE_STATUS + sub.getClientId() + "_" + pubMsg.getGuid().toString());
            //初始化 订阅者+消息 状态为PUB_TO_SUBER
            if (!rBucket_sub.isExists()) {
                rBucket_sub.set(new TrackedMessage(PUB_TO_SUBER), 1, TimeUnit.DAYS);
            }
            MqttQoS qos = ProtocolProcessor.lowerQosToTheSubscriptionDesired(sub, publishingQos);
            //该连接是否在本机
            boolean targetIsActive = this.connectionDescriptors.isConnected(sub.getClientId());
            //该连接是否在线
            boolean isOnline = m_sessionsStore.getSessionStatus(sub.getClientId());
            ClientSession targetSession = m_sessionsStore.sessionForClient(sub.getClientId());
//TODO move all this logic into messageSender, which puts into the flightZone only the messages that pull out of the queue.

            LOG.info("msg state is :" + rBucket_sub.get().getMessageStatus());
            //若连接在本机&&消息未被发送完成
            if (targetIsActive && !rBucket_sub.get().getMessageStatus().equals(COMPLETED)) {
                LOG.debug("Sending PUBLISH message to active subscriber. CId={}, topicFilter={}, qos={}",
                        sub.getClientId(), sub.getTopicFilter(), qos);
                // we need to retain because duplicate only 'copy r/w indexes and don't retain() causing
                // refCnt = 0
                ByteBuf payload = origPayload.retainedDuplicate();
                MqttPublishMessage publishMsg;
                if (qos != MqttQoS.AT_MOST_ONCE) {
                    // QoS 1 or 2
                    int messageId = targetSession.inFlightAckWaiting(pubMsg);
                    // set the PacketIdentifier only for QoS > 0
                    publishMsg = notRetainedPublishWithMessageId(topic1, qos, payload, messageId);
                } else {
                    publishMsg = notRetainedPublish(topic1, qos, payload);
                }
                this.messageSender.sendPublish(targetSession, publishMsg, pubMsg.getGuid());
            } else if (!isOnline && !rBucket_sub.get().getMessageStatus().equals(COMPLETED) && !rBucket_sub.get().getMessageStatus().equals(PUB_OFFLINE)) {
                if (!targetSession.isCleanSession()) {
                    rBucket_sub.set(new TrackedMessage(PUB_OFFLINE), 1, TimeUnit.DAYS);
                    targetSession.enqueue(pubMsg);
                }
            }
        }
    }

}

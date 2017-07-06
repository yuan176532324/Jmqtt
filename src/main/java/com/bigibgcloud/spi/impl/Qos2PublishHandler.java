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

package com.bigibgcloud.spi.impl;

import com.bigbigcloud.common.model.StoredMessage;
import com.bigibgcloud.server.ConnectionDescriptorStore;
import com.bigibgcloud.spi.IMessagesStore;
import com.bigibgcloud.spi.ISessionsStore;
import com.bigibgcloud.spi.impl.subscriptions.SubscriptionsDirectory;
import com.bigibgcloud.spi.impl.subscriptions.Topic;
import com.bigibgcloud.spi.security.IAuthorizator;
import com.bigibgcloud.server.netty.NettyUtils;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

class Qos2PublishHandler extends QosPublishHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Qos2PublishHandler.class);

    private final SubscriptionsDirectory subscriptions;
    private final IMessagesStore m_messagesStore;
    private final BrokerInterceptor m_interceptor;
    private final ConnectionDescriptorStore connectionDescriptors;
    private final ISessionsStore m_sessionsStore;
    private final MessagesPublisher publisher;

    public Qos2PublishHandler(IAuthorizator authorizator, SubscriptionsDirectory subscriptions,
                              IMessagesStore messagesStore, BrokerInterceptor interceptor,
                              ConnectionDescriptorStore connectionDescriptors, ISessionsStore sessionsStore,
                              MessagesPublisher messagesPublisher) {
        super(authorizator);
        this.subscriptions = subscriptions;
        this.m_messagesStore = messagesStore;
        this.m_interceptor = interceptor;
        this.connectionDescriptors = connectionDescriptors;
        this.m_sessionsStore = sessionsStore;
        this.publisher = messagesPublisher;
    }

    void receivedPublishQos2(Channel channel, MqttPublishMessage msg) {
        final Topic topic = new Topic(msg.variableHeader().topicName());
        // check if the topic can be wrote
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        if (!m_authorizator.canWrite(topic, username, clientID)) {
            LOG.error("MQTT client is not authorized to publish on topic. CId={}, topic={}", clientID, topic);
            return;
        }
        final int messageID = msg.variableHeader().messageId();

        StoredMessage toStoreMsg = ProtocolProcessor.asStoredMessage(msg);
        toStoreMsg.setClientID(clientID);
        LOG.info("Sending publish message to subscribers CId={}, topic={}, messageId={}", clientID, topic, messageID);
        m_sessionsStore.sessionForClient(clientID).markAsInboundInflight(messageID, toStoreMsg);
        sendPubRec(clientID, messageID);
    }

    /**
     * Second phase of a publish QoS2 protocol, sent by publisher to the broker. Search the stored
     * message and publish to all interested subscribers.
     */
    void processPubRel(Channel channel, MqttMessage msg) {
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        int messageID = Utils.messageId(msg);
        LOG.info("Processing PUBREL message. CId={}, messageId={}", clientID, messageID);
        sendPubComp(clientID, messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, false, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader("p2p", messageID);
        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, EMPTY_BUFFER);
        m_interceptor.notifyTopicPublished(publishMessage, clientID, username);
    }

    private void sendPubRec(String clientID, int messageID) {
        LOG.debug("Sending PUBREC message. CId={}, messageId={}", clientID, messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, AT_MOST_ONCE, false, 0);
        MqttMessage pubRecMessage = new MqttMessage(fixedHeader, from(messageID));
        connectionDescriptors.sendMessage(pubRecMessage, messageID, clientID);
    }

    private void sendPubComp(String clientID, int messageID) {
        LOG.debug("Sending PUBCOMP message. CId={}, messageId={}", clientID, messageID);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, AT_MOST_ONCE, false, 0);
        MqttMessage pubCompMessage = new MqttMessage(fixedHeader, from(messageID));
        connectionDescriptors.sendMessage(pubCompMessage, messageID, clientID);
    }
}

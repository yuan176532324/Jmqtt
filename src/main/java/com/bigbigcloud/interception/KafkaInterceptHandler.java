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

package com.bigbigcloud.interception;

import com.bigbigcloud.common.model.StoredMessage;
import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.server.config.KafkaConfig;
import com.bigbigcloud.spi.impl.subscriptions.Topic;
import com.bigbigcloud.BrokerConstants;
import com.bigbigcloud.interception.messages.InterceptConnectMessage;
import com.bigbigcloud.interception.messages.InterceptPublishMessage;
import com.bigbigcloud.spi.impl.Utils;
import com.bigbigcloud.interception.messages.InterceptDisconnectMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

import static com.bigbigcloud.BrokerConstants.*;

public class KafkaInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInterceptHandler.class);
    private static final Producer<String, KafkaMsg> producer4p2p;
    private static final Producer<String, DeviceConnMsg> producer4conn;
    private static final KafkaConfig kafkaConfig4p2p = new KafkaConfig(CONFIG_LOCATION + KAFKA_CONFIG_FOR_P2P);
    private static final KafkaConfig kafkaConfig4conn = new KafkaConfig(CONFIG_LOCATION + KAFKA_CONFIG_FOR_CONN);
    private ConcurrentMap<Topic, StoredMessage> m_retainedStore = RedissonUtil.getRedisson().getMap(RETAINED_STORE);

    static {
        producer4p2p = new KafkaProducer<String, KafkaMsg>(kafkaConfig4p2p.load());
        producer4conn = new KafkaProducer<String, DeviceConnMsg>(kafkaConfig4conn.load());
    }

    @Override
    public String getID() {
        return null;
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        // TODO ugly, too much array copy
        ByteBuf payload = msg.getPayload();
        byte[] payloadContent = Utils.readBytesAndRewind(payload);
        LOG.info("{} publish on {} message: {}", msg.getClientID(), msg.getTopicName(), new String(payloadContent));
        KafkaMsg mqttMessage = new KafkaMsg(msg);
        ProducerRecord<String, KafkaMsg> record = new ProducerRecord<String, KafkaMsg>(BrokerConstants.KAFKA_TOPIC_P2P, mqttMessage);
        String topicName = getTopicName(mqttMessage.getTopic());
        switch (topicName) {
            case "p2p":
                final Topic topic = new Topic(mqttMessage.getTopic());
                if (mqttMessage.getQos() == 0 && mqttMessage.isRetained()) {
                    if (m_retainedStore.containsKey(topic)) {
                        m_retainedStore.remove(topic);
                    }
                } else if (mqttMessage.getQos() == 1 && mqttMessage.isRetained()) {
                    StoredMessage storedMessage = new StoredMessage(mqttMessage.getPayload(), MqttQoS.valueOf(mqttMessage.getQos()), topicName);
                    storedMessage.setRetained(mqttMessage.isRetained());
                    storedMessage.setClientID(mqttMessage.getClientId());
                    m_retainedStore.put(topic, storedMessage);
                } else if (mqttMessage.getQos() == 2) {
                    ConcurrentMap<Integer, StoredMessage> messageIdToGuid = RedissonUtil.getRedisson().getMap(INBOUND_INFLIGHT + mqttMessage.getClientId());
                    StoredMessage storedMessage = messageIdToGuid.get(mqttMessage.getMessageId());
                    if (storedMessage.isRetained()) {
                        if (storedMessage.getPayload().readableBytes() == 0) {
                            m_retainedStore.remove(topic);
                        } else {
                            m_retainedStore.put(topic, storedMessage);
                        }
                    }
                }
                break;
            default:
                break;
        }
        producer4p2p.send(record);
    }

    private String getTopicName(String topic) {
        int index = topic.indexOf('/');
        if (index > -1) {
            return topic.substring(0, index);
        }
        return topic;
    }

    @Override
    public void onConnect(InterceptConnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                DeviceConnMsg deviceConnMsg = new DeviceConnMsg();
                deviceConnMsg.setIp(msg.getIp());
                deviceConnMsg.setClientId(msg.getClientID());
                deviceConnMsg.setDeviceGuid(strs[2]);
                deviceConnMsg.setFlag(true);
                //$SYS/presence/connected/{deviceGuid}
                ProducerRecord<String, DeviceConnMsg> record = new ProducerRecord<String, DeviceConnMsg>("SYS", deviceConnMsg);
                producer4conn.send(record);
            }
        }
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                DeviceConnMsg deviceConnMsg = new DeviceConnMsg();
                deviceConnMsg.setIp(msg.getIp());
                deviceConnMsg.setClientId(msg.getClientID());
                deviceConnMsg.setDeviceGuid(strs[2]);
                deviceConnMsg.setFlag(false);
                //$SYS/presence/connected/{deviceGuid}
                ProducerRecord<String, DeviceConnMsg> record = new ProducerRecord<String, DeviceConnMsg>("SYS", deviceConnMsg);
                producer4conn.send(record);
            }
        }
    }
}

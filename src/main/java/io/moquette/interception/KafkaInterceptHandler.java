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

package io.moquette.interception;

import com.bigbigcloud.common.model.StoredMessage;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.persistence.redis.RedissonUtil;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;

import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class KafkaInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInterceptHandler.class);
    private static final Producer<String, KafkaMsg> producer;
    private static final Producer<String, DeviceConnMsg> producer1;

    private static final String retainedStore = "retained";
    private static final String inboundInflight = "inboundInflight_";
    private ConcurrentMap<Topic, StoredMessage> m_retainedStore = RedissonUtil.getRedisson().getMap(retainedStore);

    static {
        Properties producerProperties = new Properties();
        Properties producerProperties1 = new Properties();
        InputStream in = ClassLoader.getSystemResourceAsStream("kafkaProducer.properties");
        InputStream in1 = ClassLoader.getSystemResourceAsStream("kafkaProducer4Con.properties");
        try {
            producerProperties.load(in);
            producerProperties1.load(in1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, KafkaMsg>(producerProperties);
        producer1 = new KafkaProducer<String, DeviceConnMsg>(producerProperties1);
    }

    @Override
    public String getID() {
        return null;
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        // TODO ugly, too much array copy
        ByteBuf payload = msg.getPayload();
        byte[] payloadContent = readBytesAndRewind(payload);
        LOG.info("{} publish on {} message: {}", msg.getClientID(), msg.getTopicName(), new String(payloadContent));
        KafkaMsg mqttMessage = new KafkaMsg(msg);
        ProducerRecord<String, KafkaMsg> record = new ProducerRecord<String, KafkaMsg>("p2pOut1", mqttMessage);
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
                    ConcurrentMap<Integer, StoredMessage> messageIdToGuid = RedissonUtil.getRedisson().getMap(inboundInflight + mqttMessage.getClientId());
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

        Thread thread = new Thread(() -> {
            producer.send(record);

        });
        thread.start();
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
                producer1.send(record);
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
                producer1.send(record);
            }
        }
    }
}

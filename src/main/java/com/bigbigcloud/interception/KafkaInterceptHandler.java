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

import com.bigbigcloud.interception.messages.InterceptConnectionLostMessage;
import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.persistence.redis.TrackedMessage;
import com.bigbigcloud.server.config.KafkaConfig;
import com.bigbigcloud.BrokerConstants;
import com.bigbigcloud.interception.messages.InterceptConnectMessage;
import com.bigbigcloud.interception.messages.InterceptPublishMessage;
import com.bigbigcloud.spi.impl.Utils;
import com.bigbigcloud.interception.messages.InterceptDisconnectMessage;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.redisson.api.RBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import static com.bigbigcloud.BrokerConstants.*;
import static com.bigbigcloud.persistence.redis.MessageStatus.PUB_TO_MQ;

public class KafkaInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInterceptHandler.class);
    private static final Producer<String, KafkaMsg> producer4p2p;
    private static final KafkaConfig kafkaConfig4p2p = new KafkaConfig(CONFIG_LOCATION + KAFKA_CONFIG_FOR_P2P);

    static {
        producer4p2p = new KafkaProducer<String, KafkaMsg>(kafkaConfig4p2p.load());
    }

    @Override
    public String getID() {
        return null;
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        ByteBuf payload = msg.getPayload();
        byte[] payloadContent = Utils.readBytesAndRewind(payload);
        LOG.info("{} publish on {} message: {} timestamp: {}", msg.getClientID(), msg.getTopicName(), new String(payloadContent), new Date().getTime());
        KafkaMsg mqttMessage = new KafkaMsg(msg);
        ProducerRecord<String, KafkaMsg> record = new ProducerRecord<String, KafkaMsg>(BrokerConstants.KAFKA_TOPIC_P2P, mqttMessage);
        String[] strs = msg.getClientID().split(":");
        String[] strings = msg.getTopicName().split("/");
        if (strs.length >= 3) {
            if (msg.getClientID().contains("device:")) {
                mqttMessage.setsProductKey(strs[1]);
                mqttMessage.setsDeviceGuid(strs[2]);
            } else if (msg.getClientID().contains("app:")) {
                mqttMessage.setsAppId(strs[1]);
                mqttMessage.setsUserId(strs[2]);
            }
        }
        if (strings.length >= 3) {
            String NTF = "ntf";
            String CMD = "cmd";
            if (strings[2].equals(CMD)) {
                mqttMessage.setType(0);
            } else if (strings[2].equals(NTF)) {
                mqttMessage.setType(1);
            } else {
                mqttMessage.setType(-1);
            }
        }
        LOG.info("published to mq {} ", mqttMessage.toString());
        producer4p2p.send(record);
        RBucket<TrackedMessage> rBucket = RedissonUtil.getRedisson().getBucket(MESSAGE_STATUS + mqttMessage.getClientId() + "_" + mqttMessage.getMessageId() + "_" + mqttMessage.getGuid().toString());
        rBucket.set(new TrackedMessage(PUB_TO_MQ), 7, TimeUnit.DAYS);
    }

    @Override
    public void onConnect(InterceptConnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                String on = "$SYS/presence/connected/" + strs[2];
                connMsg(msg.getClientID(), strs[1], strs[2], msg.getIp(), 1, on);
            }
        }
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                String off = "$SYS/presence/disconnected/" + strs[2];
                connMsg(msg.getClientID(), strs[1], strs[2], msg.getIp(), 0, off);
            }
        }
    }

    @Override
    public void onConnectionLost(InterceptConnectionLostMessage msg) {
        if (msg.getClientID().contains("device:")) {
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                String off = "$SYS/presence/disconnected/" + strs[2];
                connMsg(msg.getClientID(), strs[1], strs[2], msg.getIp(), 0, off);
            }
        }
    }

    private void connMsg(String clientId, String proKey, String guid, String ip, Integer type, String message) {
        KafkaMsg kafkaMsg = new KafkaMsg();
        kafkaMsg.setClientId(clientId);
        kafkaMsg.setsProductKey(proKey);
        kafkaMsg.setsDeviceGuid(guid);
        kafkaMsg.setTopic(message);
        kafkaMsg.setQos(0);
        kafkaMsg.setSource(1);
        kafkaMsg.setType(2);
        kafkaMsg.setTimestamp(System.currentTimeMillis());
        DeviceConnMsg deviceConnMsg = new DeviceConnMsg();
        deviceConnMsg.setType(type);
        deviceConnMsg.setTs(System.currentTimeMillis());
        deviceConnMsg.setIp(ip);
        kafkaMsg.setPayload(new Gson().toJson(deviceConnMsg).getBytes());
        ProducerRecord<String, KafkaMsg> record = new ProducerRecord<String, KafkaMsg>(BrokerConstants.KAFKA_TOPIC_P2P, kafkaMsg);
        producer4p2p.send(record);
    }
}

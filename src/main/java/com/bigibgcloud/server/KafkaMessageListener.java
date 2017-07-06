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

package com.bigibgcloud.server;


import com.bigbigcloud.common.json.GsonFactory;
import com.bigibgcloud.interception.KafkaMsg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageListener extends MessageListener<KafkaMsg> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageListener.class);

    private final Server server;
    private final Consumer<String, KafkaMsg> consumer;

    public KafkaMessageListener(Server server, Consumer<String, KafkaMsg> consumer) {
        this.server = server;
        this.consumer = consumer;
    }

    @Override
    protected KafkaMsg fromBytes(byte[] bytes) throws Exception {
        return GsonFactory.createGson().fromJson(new String(bytes, "UTF-8"), KafkaMsg.class);
    }

    @Override
    public void run() {
        LOG.info("begin to consume!");
//        final int giveUp = 100;
//        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, KafkaMsg> consumerRecords = consumer.poll(10000);

//            if (consumerRecords.count() == 0) {
//                noRecordsCount++;
//                if (noRecordsCount > giveUp) break;
//                else continue;
//            }

            consumerRecords.forEach(record -> {
                try {
                    LOG.info("consume the message!");
                    processMessageInternal(record.value());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            consumer.commitAsync();
        }
    }

    @Override
    protected void processMessageInternal(KafkaMsg kafkaMsg) throws Exception {
        try {
            LOG.info("{} received from kafka for topic {} message: {}", kafkaMsg.getClientId(), kafkaMsg.getTopic(),
                    new String(kafkaMsg.getPayload()));
            // TODO pass forward this information in somehow publishMessage.setLocal(false);

            MqttQoS qos = MqttQoS.valueOf(kafkaMsg.getQos());
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
            MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(kafkaMsg.getTopic(), 0);
            ByteBuf payload = Unpooled.wrappedBuffer(kafkaMsg.getPayload());
            MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
            server.internalPublish(publishMessage, kafkaMsg.getClientId());
        } catch (Exception ex) {
            LOG.error("error polling kafka msg queue", ex);
        }
    }
}

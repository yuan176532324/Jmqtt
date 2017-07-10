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

package com.bigbigcloud.server;

import com.bigbigcloud.interception.KafkaMessageConverter;
import com.bigbigcloud.interception.KafkaMsg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class ConsumerThread implements Runnable {

    private KafkaStream stream;
    private int threadNumber;
    private final Server server;
    private KafkaMessageConverter kafkaMessageConverter;
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    public ConsumerThread(KafkaStream stream, int threadNumber, Server server) {
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.server = server;
        this.kafkaMessageConverter = new KafkaMessageConverter();
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            byte[] bytes = it.next().message();
            KafkaMsg kafkaMsg = kafkaMessageConverter.fromBytes(bytes);

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
        System.out.println("Shutting down thread: " + threadNumber);
    }

}
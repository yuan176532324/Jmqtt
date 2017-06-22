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

import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class KafkaInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInterceptHandler.class);
    private static final Producer<String, KafkaMsg> producer;

    static {
        Properties producerProperties = new Properties();
        InputStream in = ClassLoader.getSystemResourceAsStream("kafkaProducer.properties");
        try {
            producerProperties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, KafkaMsg>(producerProperties);
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

        KafkaMsg kafkaMsg = new KafkaMsg(msg);
        ProducerRecord<String, KafkaMsg> record = new ProducerRecord<String, KafkaMsg>("p2pIn", kafkaMsg);
        producer.send(record);

    }

}

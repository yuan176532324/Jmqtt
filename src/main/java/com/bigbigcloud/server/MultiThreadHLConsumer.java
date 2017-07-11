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


import com.bigbigcloud.BrokerConstants;
import com.bigbigcloud.server.config.KafkaConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadHLConsumer {

    private ExecutorService executor;
    private final ConsumerConnector consumer;
    private final String topic;
    private final Server server;

    public MultiThreadHLConsumer(String topic, Server server) {
        KafkaConfig kafkaConfig = new KafkaConfig(BrokerConstants.CONFIG_LOCATION + BrokerConstants.KAFKA_CONFIG_FOR_P2P);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaConfig.load()));
        this.topic = topic;
        this.server = server;
    }

    public void testConsumer(int threadCount) {
        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(topic, threadCount);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);

        long time1 = new Date().getTime();

        executor = Executors.newFixedThreadPool(threadCount);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerThread(stream, threadNumber, server));
            threadNumber++;
        }

//        try { // without this wait the subsequent shutdown happens immediately before any messages are delivered
//            Thread.sleep(10000);
//        } catch (InterruptedException ie) {
//
//        }
//        if (consumer != null) {
//            consumer.shutdown();
//        }
//        if (executor != null) {
//            executor.shutdown();
//        }
    }
}

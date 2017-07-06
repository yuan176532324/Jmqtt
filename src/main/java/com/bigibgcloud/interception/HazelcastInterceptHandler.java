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

package com.bigibgcloud.interception;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.bigibgcloud.interception.messages.InterceptPublishMessage;
import com.bigibgcloud.server.Server;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.bigibgcloud.spi.impl.Utils.readBytesAndRewind;

public class HazelcastInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastInterceptHandler.class);
    private final HazelcastInstance hz;

    public HazelcastInterceptHandler(Server server) {
        this.hz = server.getHazelcastInstance();
    }

    @Override
    public String getID() {
        return HazelcastInterceptHandler.class.getName() + "@" + hz.getName();
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        // TODO ugly, too much array copy
        ByteBuf payload = msg.getPayload();
        byte[] payloadContent = readBytesAndRewind(payload);

        LOG.info("{} publish on {} message: {}", msg.getClientID(), msg.getTopicName(), new String(payloadContent));
        ITopic<KafkaMsg> topic = hz.getTopic("moquette");
        KafkaMsg kafkaMsg = new KafkaMsg(msg);
        topic.publish(kafkaMsg);
    }

}

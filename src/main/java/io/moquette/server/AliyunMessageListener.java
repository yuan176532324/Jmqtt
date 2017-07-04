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

package io.moquette.server;


import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.bigbigcloud.common.json.GsonFactory;
import io.moquette.interception.KafkaMsg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AliyunMessageListener extends AbstractInternalMessageListener<KafkaMsg> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunMessageListener.class);

    private final Server server;

    public AliyunMessageListener(Server server) {
        this.server = server;
    }

    @Override
    protected KafkaMsg fromBytes(byte[] bytes) throws Exception {
        return GsonFactory.createGson().fromJson(new String(bytes, "UTF-8"), KafkaMsg.class);
    }

    @Override
    protected void processMessageInternal(KafkaMsg kafkaMsg, Message message, ConsumeContext context) throws Exception {
        try {
            LOG.info("{} received from aliyunMQ for topic {} message: {} time4: {}", kafkaMsg.getClientId(), kafkaMsg.getTopic(),
                    new String(kafkaMsg.getPayload()), new Date().getTime());
            // TODO pass forward this information in somehow publishMessage.setLocal(false);

            MqttQoS qos = MqttQoS.valueOf(kafkaMsg.getQos());
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
            MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(kafkaMsg.getTopic(), 0);
            ByteBuf payload = Unpooled.wrappedBuffer(kafkaMsg.getPayload());
            MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
            server.internalPublish(publishMessage, kafkaMsg.getClientId());
        } catch (Exception ex) {
            LOG.error("error polling aliyunMQ msg queue", ex);
        }
    }
}

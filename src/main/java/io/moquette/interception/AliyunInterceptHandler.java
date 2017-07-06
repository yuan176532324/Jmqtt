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

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.bigbigcloud.common.json.GsonFactory;
import io.moquette.MqConfig;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Date;
import java.util.Properties;

import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class AliyunInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunInterceptHandler.class);
    private static final Producer producer;
    private static int count = 300;

    static {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.ProducerId, MqConfig.PRODUCER_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MqConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MqConfig.SECRET_KEY);
        producerProperties.setProperty(PropertyKeyConst.ONSAddr, MqConfig.ONSADDR);
        producerProperties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        producer = ONSFactory.createProducer(producerProperties);
        producer.start();
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
        LOG.info("{} publish on {} message: {} ", msg.getClientID(), msg.getTopicName(), new String(payloadContent));


        KafkaMsg hazelcastMsg = new KafkaMsg(msg);
        Message message = new Message(MqConfig.TOPIC, MqConfig.TAG1, GsonFactory.createGson().toJson(hazelcastMsg).getBytes());
        LOG.info("time1:" + new Date().getTime());
        SendResult sendResult = producer.send(message);

        LOG.info("time2:" + new Date().getTime());
    }

    @Override
    public void onConnect(InterceptConnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            LOG.info("send conn message!");
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                DeviceConnMsg deviceConnMsg = new DeviceConnMsg();
                deviceConnMsg.setIp(msg.getIp());
                deviceConnMsg.setClientId(msg.getClientID());
                deviceConnMsg.setDeviceGuid(strs[2]);
                deviceConnMsg.setFlag(true);
                //$SYS/presence/connected/{deviceGuid}
                Message message = new Message(MqConfig.TOPIC, MqConfig.TAG3, GsonFactory.createGson().toJson(deviceConnMsg).getBytes());
                producer.send(message);
            }
        }
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {
        if (msg.getClientID().contains("device:")) {
            LOG.info("send disconn message!");
            String[] strs = msg.getClientID().split(":");
            if (strs.length >= 3) {
                DeviceConnMsg deviceConnMsg = new DeviceConnMsg();
                deviceConnMsg.setIp(msg.getIp());
                deviceConnMsg.setClientId(msg.getClientID());
                deviceConnMsg.setDeviceGuid(strs[2]);
                deviceConnMsg.setFlag(false);
                //$SYS/presence/connected/{deviceGuid}
                Message message = new Message(MqConfig.TOPIC, MqConfig.TAG3, GsonFactory.createGson().toJson(deviceConnMsg).getBytes());
                producer.send(message);
            }
        }
    }
}

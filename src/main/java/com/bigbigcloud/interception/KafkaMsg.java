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

import com.bigbigcloud.interception.messages.InterceptPublishMessage;
import com.bigbigcloud.spi.impl.Utils;

import java.io.Serializable;
import java.util.Arrays;

public class KafkaMsg implements Serializable {

    private static final long serialVersionUID = -1431584750134928273L;
    private int qos;
    private byte[] payload;
    private String topic;
    private boolean retained;
    private String clientId;
    private int messageId;

    public KafkaMsg() {
    }

    public KafkaMsg(InterceptPublishMessage msg) {
        this.clientId = msg.getClientID();
        this.topic = msg.getTopicName();
        this.qos = msg.getQos().value();
        this.payload = Utils.readBytesAndRewind(msg.getPayload());
        this.retained = msg.isRetainFlag();
        this.messageId = msg.getMessageId();
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setRetained(boolean retained) {
        this.retained = retained;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getQos() {
        return qos;
    }

    public byte[] getPayload() {
        return payload;
    }

    public boolean isRetained() {
        return retained;
    }

    public String getTopic() {
        return topic;
    }

    public int getMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        return "KafkaMsg{" +
                "qos=" + qos +
                ", payload=" + Arrays.toString(payload) +
                ", topic='" + topic + '\'' +
                ", retained=" + retained +
                ", clientId='" + clientId + '\'' +
                ", messageId=" + messageId +
                '}';
    }
}

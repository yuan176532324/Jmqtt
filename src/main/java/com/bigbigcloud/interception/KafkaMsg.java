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

import com.bigbigcloud.common.model.MessageGUID;
import com.bigbigcloud.interception.messages.InterceptPublishMessage;
import com.bigbigcloud.spi.impl.Utils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

public class KafkaMsg implements Serializable {

    private static final long serialVersionUID = -1431584750134928273L;
    private Integer qos = 0;
    private byte[] payload;
    private String topic;
    private Boolean retained;
    private String clientId;
    private Integer messageId = 0;
    private MessageGUID guid;
    private String sDeviceGuid;
    private String sAppId;
    private String sUserId;
    private String sProductKey;
    private Integer source;//dh/mqtt 0/1
    private Integer type;//cmd/ntf/sys/unknown 0/1/2/-1
    private Long timestamp;

    public KafkaMsg() {
    }

    KafkaMsg(InterceptPublishMessage msg) {
        this.clientId = msg.getClientID();
        this.topic = msg.getTopicName();
        this.qos = msg.getQos().value();
        this.payload = Utils.readBytesAndRewind(msg.getPayload());
        this.retained = msg.isRetainFlag();
        this.messageId = msg.getMessageId();
        this.guid = new MessageGUID(UUID.randomUUID().toString());
        this.source = 1;
        this.timestamp = System.currentTimeMillis();
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getsDeviceGuid() {
        return sDeviceGuid;
    }

    public void setsDeviceGuid(String sDeviceGuid) {
        this.sDeviceGuid = sDeviceGuid;
    }

    public String getsAppId() {
        return sAppId;
    }

    public void setsAppId(String sAppId) {
        this.sAppId = sAppId;
    }

    public String getsUserId() {
        return sUserId;
    }

    public void setsUserId(String sUserId) {
        this.sUserId = sUserId;
    }

    public String getsProductKey() {
        return sProductKey;
    }

    public void setsProductKey(String sProductKey) {
        this.sProductKey = sProductKey;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public MessageGUID getGuid() {
        return guid;
    }

    public void setGuid(MessageGUID guid) {
        this.guid = guid;
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
                ", guid=" + guid +
                ", sDeviceGuid='" + sDeviceGuid + '\'' +
                ", sAppId='" + sAppId + '\'' +
                ", sUserId='" + sUserId + '\'' +
                ", sProductKey='" + sProductKey + '\'' +
                ", source=" + source +
                ", type=" + type +
                '}';
    }
}

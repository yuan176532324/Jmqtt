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

import java.io.Serializable;
import java.util.UUID;

import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class HazelcastMsg implements Serializable {

    private static final long serialVersionUID = -1431584750134928273L;
    private int qos;
    private byte[] payload;
    private String topic;
    private boolean retained;
    private String clientId;
    private int messageId;

    public HazelcastMsg(InterceptPublishMessage msg) {
        this.clientId = msg.getClientID();
        this.topic = msg.getTopicName();
        this.qos = msg.getQos().value();
        this.payload = readBytesAndRewind(msg.getPayload());
        this.retained = msg.isRetainFlag();
        this.messageId = msg.getMessageId();
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
}

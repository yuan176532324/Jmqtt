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

package io.moquette.spi;

import com.bigbigcloud.common.model.StoredMessage;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Defines the SPI to be implemented by a StorageService that handle persistence of messages
 */
public interface IMessagesStore {

    /**
     * Used to initialize all persistent store structures
     */
    void initStore();

    /**
     * Return a list of retained messages that satisfy the condition.
     *
     * @param condition the condition to match during the search.
     * @return the collection of matching messages.
     */
    Collection<StoredMessage> searchMatching(IMatchingCondition condition);

    void cleanRetained(Topic topic);

    void storeRetained(Topic topic, StoredMessage storedMessage);
}

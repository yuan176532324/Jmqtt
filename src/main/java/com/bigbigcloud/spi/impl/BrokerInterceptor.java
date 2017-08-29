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

package com.bigbigcloud.spi.impl;

import com.bigbigcloud.interception.messages.InterceptConnectionLostMessage;
import com.bigbigcloud.interception.messages.InterceptUnsubscribeMessage;
import com.bigbigcloud.BrokerConstants;
import com.bigbigcloud.interception.InterceptHandler;
import com.bigbigcloud.interception.Interceptor;
import com.bigbigcloud.logging.LoggingUtils;
import com.bigbigcloud.server.config.IConfig;
import com.bigbigcloud.spi.impl.subscriptions.Subscription;
import com.bigbigcloud.interception.messages.InterceptAcknowledgedMessage;
import com.bigbigcloud.interception.messages.InterceptConnectMessage;
import com.bigbigcloud.interception.messages.InterceptDisconnectMessage;
import com.bigbigcloud.interception.messages.InterceptPublishMessage;
import com.bigbigcloud.interception.messages.InterceptSubscribeMessage;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An interceptor that execute the interception tasks asynchronously.
 */
final class BrokerInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerInterceptor.class);
    private final Map<Class<?>, List<InterceptHandler>> handlers;
    private final ExecutorService executor;

    private BrokerInterceptor(int poolSize, List<InterceptHandler> handlers) {
        LOG.info("Initializing broker interceptor. InterceptorIds={}", LoggingUtils.getInterceptorIds(handlers));
        this.handlers = new HashMap<>();
        for (Class<?> messageType : InterceptHandler.ALL_MESSAGE_TYPES) {
            this.handlers.put(messageType, new CopyOnWriteArrayList<InterceptHandler>());
        }
        for (InterceptHandler handler : handlers) {
            this.addInterceptHandler(handler);
        }
        executor = Executors.newFixedThreadPool(poolSize);
    }

    /**
     * Configures a broker interceptor, with a thread pool of one thread.
     *
     * @param handlers
     */
    BrokerInterceptor(List<InterceptHandler> handlers) {
        this(1, handlers);
    }

    /**
     * Configures a broker interceptor using the pool size specified in the IConfig argument.
     */
    BrokerInterceptor(IConfig props, List<InterceptHandler> handlers) {
        this(Integer.parseInt(props.getProperty(BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE, "1")), handlers);
    }

    private static Class<?>[] getInterceptedMessageTypes(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = interceptHandler.getInterceptedMessageTypes();
        if (interceptedMessageTypes == null) {
            return InterceptHandler.ALL_MESSAGE_TYPES;
        }
        return interceptedMessageTypes;
    }

    /**
     * Shutdown graciously the executor service
     */
    void stop() {
        LOG.info("Shutting down interceptor thread pool...");
        executor.shutdown();
        try {
            LOG.info("Waiting for thread pool tasks to terminate...");
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        if (!executor.isTerminated()) {
            LOG.warn("Forcing shutdown of interceptor thread pool...");
            executor.shutdownNow();
        }
    }

    @Override
    public void notifyClientConnected(final MqttConnectMessage msg, Channel channel) {
        for (final InterceptHandler handler : this.handlers.get(InterceptConnectMessage.class)) {
            LOG.debug("Sending MQTT CONNECT message to interceptor. CId={}, interceptorId={}",
                    msg.payload().clientIdentifier(), handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onConnect(new InterceptConnectMessage(msg, channel.remoteAddress().toString()));
                }
            });
        }
    }

    @Override
    public void notifyClientDisconnected(final String clientID, final String username, final String ip) {
        for (final InterceptHandler handler : this.handlers.get(InterceptDisconnectMessage.class)) {
            LOG.debug("Notifying MQTT client disconnection to interceptor. CId={}, username={}, interceptorId={}",
                    clientID, username, handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onDisconnect(new InterceptDisconnectMessage(clientID, username, ip));
                }
            });
        }
    }

    @Override
    public void notifyClientConnectionLost(final String clientID, final String username, final String ip) {
        for (final InterceptHandler handler : this.handlers.get(InterceptConnectionLostMessage.class)) {
            LOG.debug("Notifying unexpected MQTT client disconnection to interceptor CId={}, username={}, " +
                    "interceptorId={}", clientID, username, handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onConnectionLost(new InterceptConnectionLostMessage(clientID, username, ip));
                }
            });
        }
    }

    @Override
    public void notifyTopicPublished(final MqttPublishMessage msg, final String clientID, final String username) {
        int messageId = msg.variableHeader().messageId();
        String topic = msg.variableHeader().topicName();
        for (final InterceptHandler handler : this.handlers.get(InterceptPublishMessage.class)) {
            LOG.debug("Notifying MQTT PUBLISH message to interceptor. CId={}, messageId={}, topic={}, interceptorId={}",
                    clientID, messageId, topic, handler.getID());
            executor.execute(() -> handler.onPublish(new InterceptPublishMessage(msg, clientID, username)));
        }
    }

    @Override
    public void notifyTopicSubscribed(final Subscription sub, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptSubscribeMessage.class)) {
            LOG.debug("Notifying MQTT SUBSCRIBE message to interceptor. CId={}, topicFilter={}, interceptorId={}",
                    sub.getClientId(), sub.getTopicFilter(), handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onSubscribe(new InterceptSubscribeMessage(sub, username));
                }
            });
        }
    }

    @Override
    public void notifyTopicUnsubscribed(final String topic, final String clientID, final String username) {
        for (final InterceptHandler handler : this.handlers.get(InterceptUnsubscribeMessage.class)) {
            LOG.debug("Notifying MQTT UNSUBSCRIBE message to interceptor. CId={}, topic={}, interceptorId={}", clientID,
                    topic, handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onUnsubscribe(new InterceptUnsubscribeMessage(topic, clientID, username));
                }
            });
        }
    }

    @Override
    public void notifyMessageAcknowledged(final InterceptAcknowledgedMessage msg) {
        for (final InterceptHandler handler : this.handlers.get(InterceptAcknowledgedMessage.class)) {
            LOG.debug("Notifying MQTT ACK message to interceptor. CId={}, messageId={}, topic={}, interceptorId={}",
                    msg.getMsg().getClientID(), msg.getPacketID(), msg.getTopic(), handler.getID());
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    handler.onMessageAcknowledged(msg);
                }
            });
        }
    }

    @Override
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = getInterceptedMessageTypes(interceptHandler);
        LOG.info("Adding MQTT message interceptor. InterceptorId={}, handledMessageTypes={}",
                interceptHandler.getID(), interceptedMessageTypes);
        for (Class<?> interceptMessageType : interceptedMessageTypes) {
            this.handlers.get(interceptMessageType).add(interceptHandler);
        }
    }

    @Override
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        Class<?>[] interceptedMessageTypes = getInterceptedMessageTypes(interceptHandler);
        LOG.info("Removing MQTT message interceptor. InterceptorId={}, handledMessageTypes={}",
                interceptHandler.getID(), interceptedMessageTypes);
        for (Class<?> interceptMessageType : interceptedMessageTypes) {
            this.handlers.get(interceptMessageType).remove(interceptHandler);
        }
    }
}

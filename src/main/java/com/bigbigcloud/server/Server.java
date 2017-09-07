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
import com.bigbigcloud.common.model.MessageGUID;
import com.bigbigcloud.connections.IConnectionsManager;
import com.bigbigcloud.interception.*;
import com.bigbigcloud.logging.LoggingUtils;
import com.bigbigcloud.server.config.*;
import com.bigbigcloud.server.netty.NettyAcceptor;
import com.bigbigcloud.spi.impl.ProtocolProcessor;
import com.bigbigcloud.spi.impl.ProtocolProcessorBootstrapper;
import com.bigbigcloud.spi.impl.subscriptions.Subscription;
import com.bigbigcloud.spi.security.IAuthenticator;
import com.bigbigcloud.spi.security.IAuthorizator;
import com.bigbigcloud.spi.security.ISslContextCreator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Launch a configured version of the server.
 */
public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private ServerAcceptor m_acceptor;

    private volatile boolean m_initialized;

    private ProtocolProcessor m_processor;

    private KafkaMessageConverter kafkaMessageConverter = new KafkaMessageConverter();

    private ProtocolProcessorBootstrapper m_processorBootstrapper;

    private ScheduledExecutorService scheduler;

    public static void main(String[] args) throws Exception {
        final Server server = new Server();
        server.startServer();
        System.out.println("Server started, version 0.10-SNAPSHOT");
        // Bind a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                server.stopServer();
            }
        });
    }

    /**
     * Starts Moquette bringing the configuration from the file located at m_config/moquette.conf
     *
     * @throws IOException in case of any IO error.
     */
    public void startServer() throws Exception {
        File defaultConfigurationFile = defaultConfigFile();
        LOG.info("Starting Moquette server. Configuration file path={}", defaultConfigurationFile.getAbsolutePath());
        IResourceLoader filesystemLoader = new FileResourceLoader(defaultConfigurationFile);
        final IConfig config = new ResourceLoaderConfig(filesystemLoader);
        startServer(config);
    }

    private static File defaultConfigFile() {
        String configPath = System.getProperty("moquette.path", null);
        return new File(configPath, BrokerConstants.CONFIG_LOCATION + IConfig.DEFAULT_CONFIG);
    }

    /**
     * Starts Moquette bringing the configuration from the given file
     *
     * @param configFile text file that contains the configuration.
     * @throws IOException in case of any IO Error.
     */
    public void startServer(File configFile) throws Exception {
        LOG.info("Starting Moquette server. Configuration file path={}", configFile.getAbsolutePath());
        IResourceLoader filesystemLoader = new FileResourceLoader(configFile);
        final IConfig config = new ResourceLoaderConfig(filesystemLoader);
        startServer(config);
    }

    /**
     * Starts the server with the given properties.
     * <p>
     * Its suggested to at least have the following properties:
     * <ul>
     * <li>port</li>
     * <li>password_file</li>
     * </ul>
     *
     * @param configProps the properties map to use as configuration.
     * @throws IOException in case of any IO Error.
     */
    public void startServer(Properties configProps) throws Exception {
        LOG.info("Starting Moquette server using properties object");
        final IConfig config = new MemoryConfig(configProps);
        startServer(config);
    }

    /**
     * Starts Moquette bringing the configuration files from the given Config implementation.
     *
     * @param config the configuration to use to start the broker.
     * @throws IOException in case of any IO Error.
     */
    public void startServer(IConfig config) throws Exception {
        LOG.info("Starting Moquette server using IConfig instance...");
        startServer(config, null);
    }

    /**
     * Starts Moquette with config provided by an implementation of IConfig class and with the set
     * of InterceptHandler.
     *
     * @param config   the configuration to use to start the broker.
     * @param handlers the handlers to install in the broker.
     * @throws IOException in case of any IO Error.
     */
    public void startServer(IConfig config, List<? extends InterceptHandler> handlers) throws Exception {
        LOG.info("Starting moquette server using IConfig instance and intercept handlers");
        startServer(config, handlers, null, null, null);
    }

    public void startServer(IConfig config, List<? extends InterceptHandler> handlers, ISslContextCreator sslCtxCreator,
                            IAuthenticator authenticator, IAuthorizator authorizator) throws Exception {
        if (handlers == null) {
            handlers = Collections.emptyList();
        }
        LOG.info("Starting Moquette Server. MQTT message interceptors={}", LoggingUtils.getInterceptorIds(handlers));

        scheduler = Executors.newScheduledThreadPool(Integer.valueOf(config.getProperty(BrokerConstants.SERVER_THREADS)));
        config.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, KafkaInterceptHandler.class.getCanonicalName());
        final String handlerProp = System.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (handlerProp != null) {
            config.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, handlerProp);
        }
        final String persistencePath = config.getProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME);
        LOG.info("Configuring Using persistent store file, path={}", persistencePath);
        m_processorBootstrapper = new ProtocolProcessorBootstrapper();
        final ProtocolProcessor processor = m_processorBootstrapper.init(config, handlers, authenticator, authorizator,
                this);
        LOG.info("Initialized MQTT protocol processor");
        if (sslCtxCreator == null) {
            LOG.warn("Using default SSL context creator");
            sslCtxCreator = new DefaultMoquetteSslContextCreator(config);
        }

        LOG.info("Binding server to the configured ports");
        m_acceptor = new NettyAcceptor();
        m_acceptor.initialize(processor, config, sslCtxCreator);
        m_processor = processor;

        LOG.info("Moquette server has been initialized successfully");
        m_initialized = true;
        configureKafka(config);
    }

    private void configureKafka(IConfig config) throws Exception {
        String topic = config.getProperty(BrokerConstants.KAFKA_TOPIC);
        Properties props = createKafkaStreamProperties();
        KafkaStreams streams = new KafkaStreams(getStreamBuilder(topic), props);
        streams.start();
    }

    private KStreamBuilder getStreamBuilder(String topic) {
        Serde<byte[]> byteSerde = Serdes.ByteArray();
        KStreamBuilder builder = new KStreamBuilder();
        KStream<byte[], byte[]> streams = builder.stream(byteSerde, byteSerde, topic);
        streams.foreach((key, value) -> {
            try {
                KafkaMsg kafkaMsg = kafkaMessageConverter.fromBytes(value);
                LOG.info("{} received from kafka for topic {} message: {} timestamp: {}", kafkaMsg.getClientId(), kafkaMsg.getTopic(),
                        new String(kafkaMsg.getPayload()), new Date().getTime());
                MqttQoS qos = MqttQoS.valueOf(kafkaMsg.getQos());
                MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
                MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(kafkaMsg.getTopic(), kafkaMsg.getMessageId());
                ByteBuf payload = Unpooled.wrappedBuffer(kafkaMsg.getPayload());
                MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
                this.internalPublish(publishMessage, kafkaMsg.getClientId(), kafkaMsg.getGuid());
            } catch (Exception ex) {
                LOG.error("error polling kafka msg queue", ex);
            }
        });
        return builder;
    }

    private static Properties createKafkaStreamProperties() {
        KafkaConfig kafkaConfig = new KafkaConfig(BrokerConstants.CONFIG_LOCATION + BrokerConstants.KAFKA_CONFIG_FOR_P2P);
        return kafkaConfig.load();
    }

    /**
     * Use the broker to publish a message. It's intended for embedding applications. It can be used
     * only after the server is correctly started with startServer.
     *
     * @param msg      the message to forward.
     * @param clientId the id of the sending server.
     * @throws IllegalStateException if the server is not yet started
     */
    void internalPublish(MqttPublishMessage msg, final String clientId, MessageGUID messageGUID) throws IOException {
        final int messageID = msg.variableHeader().messageId();
        if (!m_initialized) {
            LOG.error("Moquette is not started, internal message cannot be published. CId={}, messageId={}", clientId,
                    messageID);
            throw new IllegalStateException("Can't publish on a server is not yet started");
        }
        LOG.debug("Publishing message. CId={}, messageId={}", clientId, messageID);
        m_processor.internalPublish(msg, clientId, messageGUID);
    }

    public void stopServer() {
        LOG.info("Unbinding server from the configured ports");
        m_acceptor.close();
        LOG.trace("Stopping MQTT protocol processor");
        m_processorBootstrapper.shutdown();
        m_initialized = false;
        scheduler.shutdown();

        LOG.info("Moquette server has been stopped.");
    }

    /**
     * SPI method used by Broker embedded applications to get list of subscribers. Returns null if
     * the broker is not started.
     *
     * @return list of subscriptions.
     */
    public List<Subscription> getSubscriptions() {
        if (m_processorBootstrapper == null) {
            return null;
        }
        return m_processorBootstrapper.getSubscriptions();
    }

    /**
     * SPI method used by Broker embedded applications to add intercept handlers.
     *
     * @param interceptHandler the handler to add.
     */
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        if (!m_initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be added. InterceptorId={}",
                    interceptHandler.getID());
            throw new IllegalStateException("Can't register interceptors on a server that is not yet started");
        }
        LOG.info("Adding MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        m_processor.addInterceptHandler(interceptHandler);
    }

    /**
     * SPI method used by Broker embedded applications to remove intercept handlers.
     *
     * @param interceptHandler the handler to remove.
     */
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        if (!m_initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be removed. InterceptorId={}",
                    interceptHandler.getID());
            throw new IllegalStateException("Can't deregister interceptors from a server that is not yet started");
        }
        LOG.info("Removing MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        m_processor.removeInterceptHandler(interceptHandler);
    }

    /**
     * Returns the connections manager of this broker.
     *
     * @return IConnectionsManager the instance used bt the broker.
     */
    public IConnectionsManager getConnectionsManager() {
        return m_processorBootstrapper.getConnectionDescriptors();
    }

    public ProtocolProcessor getProcessor() {
        return m_processor;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }
}

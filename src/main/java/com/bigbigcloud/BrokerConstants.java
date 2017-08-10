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

package com.bigbigcloud;

import java.io.File;

public final class BrokerConstants {

    public static final String INTERCEPT_HANDLER_PROPERTY_NAME = "intercept.handler";
    public static final String BROKER_INTERCEPTOR_THREAD_POOL_SIZE = "intercept.thread_pool.size";
    public static final String PERSISTENT_STORE_PROPERTY_NAME = "persistent_store";
    public static final String AUTOSAVE_INTERVAL_PROPERTY_NAME = "autosave_interval";
    public static final String PASSWORD_FILE_PROPERTY_NAME = "password_file";
    public static final String PORT_PROPERTY_NAME = "port";
    public static final String HOST_PROPERTY_NAME = "host";
    public static final String DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME = "moquette_store.mapdb";
    public static final String DEFAULT_PERSISTENT_PATH = System.getProperty("user.dir") + File.separator
            + DEFAULT_MOQUETTE_STORE_MAP_DB_FILENAME;
    public static final String WEB_SOCKET_PORT_PROPERTY_NAME = "websocket_port";
    public static final String WSS_PORT_PROPERTY_NAME = "secure_websocket_port";
    public static final String SSL_PORT_PROPERTY_NAME = "ssl_port";
    public static final String JKS_PATH_PROPERTY_NAME = "jks_path";
    public static final String KEY_STORE_PASSWORD_PROPERTY_NAME = "key_store_password";
    public static final String KEY_MANAGER_PASSWORD_PROPERTY_NAME = "key_manager_password";
    public static final String ALLOW_ANONYMOUS_PROPERTY_NAME = "allow_anonymous";
    public static final String ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME = "allow_zero_byte_client_id";
    public static final String ACL_FILE_PROPERTY_NAME = "acl_file";
    public static final String AUTHORIZATOR_CLASS_NAME = "authorizator_class";
    public static final String AUTHENTICATOR_CLASS_NAME = "authenticator_class";
    public static final String DB_AUTHENTICATOR_DRIVER = "authenticator.db.driver";
    public static final String DB_AUTHENTICATOR_URL = "authenticator.db.url";
    public static final String DB_AUTHENTICATOR_QUERY = "authenticator.db.query";
    public static final String DB_AUTHENTICATOR_DIGEST = "authenticator.db.digest";
    public static final int PORT = 1883;
    public static final int WEBSOCKET_PORT = 8080;
    public static final String DISABLED_PORT_BIND = "disabled";
    public static final String HOST = "0.0.0.0";
    public static final String NEED_CLIENT_AUTH = "need_client_auth";
    public static final String HAZELCAST_CONFIGURATION = "hazelcast.configuration";
    public static final String NETTY_SO_BACKLOG_PROPERTY_NAME = "netty.so_backlog";
    public static final String NETTY_SO_REUSEADDR_PROPERTY_NAME = "netty.so_reuseaddr";
    public static final String NETTY_TCP_NODELAY_PROPERTY_NAME = "netty.tcp_nodelay";
    public static final String NETTY_SO_KEEPALIVE_PROPERTY_NAME = "netty.so_keepalive";
    public static final String NETTY_CHANNEL_TIMEOUT_SECONDS_PROPERTY_NAME = "netty.channel_timeout.seconds";
    public static final String STORAGE_CLASS_NAME = "storage_class";
    public static final String KAFKA_TOPIC = "topic";
    public static final String KAFKA_TOPIC_P2P = "mqtt_p2pout";
    public static final String KAFKA_TOPIC_SYS = "mqtt_sys";
    public static final String THREAD_COUNTS = "threadCounts";
    public static final String SERVER_THREADS = "serverThreads";
    public static final String KAFKA_CONFIG_FOR_P2P = "kafkaConfig4P2P.properties";
    public static final String KAFKA_CONFIG_FOR_CONN = "kafkaConfig4Con.properties";
    public static final String REDIS_CONFIG = "RedisConf.json";
    public static final String RETAINED_STORE = "retained:";
    public static final String INBOUND_INFLIGHT = "inboundInflight:";
    public static final String CONFIG_LOCATION = System.getProperty("config.location");
    public static final String DEVICE = "device";
    public static final String APP = "app";
    public static final String SIGN = "sign";
    public static final String APPID = "appId";
    public static final String PRODUCTKEY = "productKey";
    public static final String AUTHSIGNATURE = "Authorization";
    public static final String AW_AUTHSIGNATURE = "AW-Authorization";
    private static final String BEARER = "Bearer";
    public static final String DH_ACCESSKEY = BEARER + "hJ9+tPuq7p81wv8yHLKDx94i+Ms2VRS0Fmnmn6w9H68=";
    public static final String APPHUB_ACCESSKEY = BEARER + "lovIosOPN3NNNflvKLnHQI594Alq/pFfjq0u50icX8E=";
    public static final String DEVICEHIVE_URL = System.getProperty("devicehive.url");
    public static final String APPHUB_URL = System.getProperty("apphub.url");
    public static final String APPHUB_PORT = System.getProperty("apphub.port");
    public static final String DEVICEHIVE_PORT = System.getProperty("devicehive.port");
    public static final long USER_NOT_FOUND = 41000;
    public static final long PASSWORD_INVALID = 40012;
    public static final int HEALTH_CHECK_PORT = 3881;
    public static final String KEY_SUBSCRIPTIONS = "subscription:";
    public static final String OUTBOUND_FLIGHT = "outboundFlight:";
    public static final String INFLIGHT_PACKETIDS = "inflightPacketID:";
    public static final String BLACKLIST = "blackList:";
    public static final String SESSION = "session:";
    public static final String SECOND_PHASE = "secondPhase:";
    public static final String DH_PATH = "devicehivePath";
    public static final String APPHUB_PATH = "apphubPath";
    public static final String MESSAGE_STATUS = "msgStatus_";
    public static final String OFFLINE_MESSAGES = "offlineMsg_";
    public static final String IN_FLIGHT = "inFlight_";
    public static final String SECOND_FLIGHT = "secFlight_";
    private BrokerConstants() {
    }
}

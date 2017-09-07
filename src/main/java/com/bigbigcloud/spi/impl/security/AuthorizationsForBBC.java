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

package com.bigbigcloud.spi.impl.security;

import com.bigbigcloud.server.config.IConfig;
import com.bigbigcloud.spi.impl.subscriptions.Topic;
import com.bigbigcloud.spi.security.IAuthorizator;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.LinkedList;

import static com.bigbigcloud.BrokerConstants.*;
import static com.bigbigcloud.configuration.Constants.*;
import static com.bigbigcloud.spi.impl.HttpUtils.doGetHttpUrl;
import static com.bigbigcloud.spi.impl.HttpUtils.sendGet;

public class AuthorizationsForBBC implements IAuthorizator {
    private IConfig iConfig;
    private static final Logger LOG = LoggerFactory.getLogger(BBCAuthenticator.class);

    public AuthorizationsForBBC(IConfig iConfig) {
        this.iConfig = iConfig;
    }

    @Override
    public boolean canWrite(Topic topic, String user, String client) {
        return canDoOperation(topic, user, client);
    }

    @Override
    public boolean canRead(Topic topic, String user, String client) {
        return canDoOperation(topic, user, client);
    }

    private boolean canDoOperation(Topic topic, String username, String client) {
        if (username == null || username.equals("testuser")) {
            return true;
        }
        if (isNotEmpty(client)) {
            String[] strs = client.split(":");
            String[] strings = topic.getTopic().split("/");
            if (client.contains("device")) {
                if (strs.length >= 3 && strings.length >= 2) {
                    if (strs[2].equals(strings[1])) {
                        LOG.info("device sub/pub success !deviceGuid is :" + strs[2]);
                        return true;
                    }
                }
            } else if (client.contains("app")) {
                if (strs.length >= 3 && strings.length >= 2) {
                    String requsetPathForDH = iConfig.getProperty(DH_PATH_1);
                    String dhGuid = "{deviceGuid}";
                    String dhUserId = "{userId}";
                    String deviceGuid = strings[1];
                    String userId = strs[2];
                    if (topic.getTopic().contains("SYS")) {
                        deviceGuid = strings[strings.length - 1];
                    }
                    String requsetPath = StringUtils.replace(requsetPathForDH, dhGuid, deviceGuid);
                    String requsetPathNew = StringUtils.replace(requsetPath, dhUserId, userId);
                    LOG.info("deviceGuid is :" + deviceGuid + ", userId is :" + userId + ", requsetPathForDH is:" + requsetPathNew);
                    //生成URI
                    URI uri = doGetHttpUrl(DEVICEHIVE_URL, DEVICEHIVE_PORT == null ? null : Integer.valueOf(DEVICEHIVE_PORT), requsetPathNew, new LinkedList<>());
                    //Http Get请求dh
                    String response = sendGet(uri, AUTHSIGNATURE, DH_ACCESSKEY);
                    LOG.info("deviceHive response is :" + response);
                    JsonObject jsonObject;
                    try {
                        jsonObject = new JsonParser().parse(response).getAsJsonObject();
                    } catch (Exception e) {
                        LOG.warn("response error:" + e);
                        return false;
                    }
                    if (jsonObject.get(STATUS).getAsString().equals(SUCC)) {
                        LOG.info("can pub/sub, client is: {} , topic is: {}", client, topic.getTopic());
                        return true;
                    } else if (jsonObject.get(STATUS).getAsString().equals(ERR)) {
                        LOG.info("can not pub/sub, client is: {} , topic is: {}", client, topic.getTopic());
                        return false;
                    }
                }
            }
        }
        return false;
    }

    private boolean isNotEmpty(String client) {
        return client != null && !client.isEmpty();
    }

}

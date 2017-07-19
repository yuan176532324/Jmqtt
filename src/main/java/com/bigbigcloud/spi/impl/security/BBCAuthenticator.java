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
import com.bigbigcloud.spi.security.IAuthenticator;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import static com.bigbigcloud.BrokerConstants.*;
import static com.bigbigcloud.BrokerConstants.APP;
import static com.bigbigcloud.configuration.Constants.*;
import static com.bigbigcloud.spi.impl.HttpUtils.doGetHttpUrl;
import static com.bigbigcloud.spi.impl.HttpUtils.sendGet;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BBCAuthenticator implements IAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(BBCAuthenticator.class);
    private IConfig iConfig;

    public BBCAuthenticator(IConfig iConfig) {
        this.iConfig = iConfig;
    }

    public boolean checkValid(String clientId, String username, byte[] password) {
        String deviceGuid;
        String productKey;
        String pwd = new String(password, UTF_8);
        String appId;
        String userId;
        //未登陆过或缓存失效，处理device和app
        final String[] strs = clientId.split(":");
        LOG.info("check valid for clientId:" + clientId);
        if (strs.length > 3) {
            String key = strs[0];
            String requsetPath;
            if (key.equals(DEVICE)) {
                LOG.info("client ask dh to login:" + clientId);
                //生成请求参数
                productKey = strs[1];
                deviceGuid = strs[2];
                String dhGuid = "{deviceGuid}";
                String requsetPathForDH = iConfig.getProperty(DH_PATH);
                requsetPath = StringUtils.replace(requsetPathForDH, dhGuid, deviceGuid);
                LOG.info("deviceGuid is :" + deviceGuid + ", requsetPathForDH is:" + requsetPath);
                List<NameValuePair> nvps = new LinkedList<>();
                nvps.add(new BasicNameValuePair(SIGN, pwd));
                nvps.add(new BasicNameValuePair(PRODUCTKEY, productKey));
                //生成URI
                URI uri = doGetHttpUrl(DEVICEHIVE_URL, DEVICEHIVE_PORT == null ? null : Integer.valueOf(DEVICEHIVE_PORT), requsetPath, nvps);
                //Http Get请求dh
                String response = sendGet(uri, AUTHSIGNATURE, DH_ACCESSKEY);
                LOG.info("dh url :{},response from dh:{}", uri, response);
                return responseParser(response, clientId, pwd);
            } else if (key.equals(APP)) {
                LOG.info("client ask apphub to login:" + clientId);
                //生成请求参数
                appId = strs[1];
                userId = strs[2];
                String user = "{userId}";
                String requsetPathForAppHub = iConfig.getProperty(APPHUB_PATH);
                requsetPath = StringUtils.replace(requsetPathForAppHub, user, userId);
                LOG.info("userId is :" + userId + ", requsetPathForAppHub is:" + requsetPath);
                List<NameValuePair> nvps = new LinkedList<>();
                nvps.add(new BasicNameValuePair(SIGN, pwd));
                nvps.add(new BasicNameValuePair(APPID, appId));
                //生成URI
                URI uri = doGetHttpUrl(APPHUB_URL, APPHUB_PORT == null ? null : Integer.valueOf(APPHUB_PORT), requsetPath, nvps);
                //Http Get请求dh
                String response = sendGet(uri, AW_AUTHSIGNATURE, APPHUB_ACCESSKEY);
                LOG.info("apphub url :{},response from apphub:{}", uri, response);
                return responseParser(response, clientId, pwd);
            }
        }
        return false;
    }

    private boolean responseParser(String response, String clientId, String password) {
        JsonObject jsonObject;
        try {
            jsonObject = new JsonParser().parse(response).getAsJsonObject();
        } catch (Exception e) {
            LOG.warn("response error:" + e);
            return false;
        }
        if (jsonObject.get(STATUS).getAsString().equals(SUCC)) {
            LOG.info("login success, client is:" + clientId);
            return true;
        } else if (jsonObject.get(STATUS).getAsString().equals(ERR)) {
            //日志根据dh返回值确定错误类型
            if (jsonObject.get(ERRORCODE).getAsLong() == USER_NOT_FOUND) {
                LOG.warn("user not found, clientId:" + clientId);
            } else if (jsonObject.get(ERRORCODE).getAsLong() == PASSWORD_INVALID) {
                LOG.warn("check invalid, pwd is:" + password);
            }
        }
        return false;
    }

}

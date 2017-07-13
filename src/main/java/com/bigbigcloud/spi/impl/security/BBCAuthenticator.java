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

import com.bigbigcloud.persistence.redis.RedissonUtil;
import com.bigbigcloud.spi.security.IAuthenticator;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.bigbigcloud.BrokerConstants.*;
import static com.bigbigcloud.BrokerConstants.APP;
import static com.bigbigcloud.configuration.Constants.*;
import static com.bigbigcloud.spi.impl.HttpUtils.doGetHttpUrl;
import static com.bigbigcloud.spi.impl.HttpUtils.sendGet;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BBCAuthenticator implements IAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(BBCAuthenticator.class);
    private String requsetPath = "/dh/v2/rest/device/{deviceGuid}/auth/mqtt";
    private RedissonClient redissonClient = RedissonUtil.getRedisson();

    private String getPassWord(String clientId) {
        return redissonClient.getBucket(REDIS_PASSWORD_STORE + clientId).get().toString();
    }

    public boolean checkValid(String clientId, String username, byte[] password) {
        String deviceGuid;
        String productKey;
        String pwd = new String(password, UTF_8);
        String appId;
        String[] strs = clientId.split(":");
        LOG.info("check valid for clientId:" + clientId);
        if (strs.length > 3) {
            if (clientId.contains(DEVICE)) {
                if (redissonClient.getBucket(REDIS_PASSWORD_STORE + clientId).isExists()) {
                    LOG.info("client had login before:" + clientId);
                    if (pwd.equals(getPassWord(clientId))) {
                        return true;
                    } else {
                        LOG.error("device check valid, pwd is:{}", pwd);
                        return false;
                    }
                } else {
                    LOG.info("client ask dh to login:" + clientId);

                    //生成请求参数
                    productKey = strs[1];
                    deviceGuid = strs[2];
                    String dhGuid = "{deviceGuid}";
                    requsetPath = StringUtils.replace(requsetPath, dhGuid, deviceGuid);
                    List<NameValuePair> nvps = new LinkedList<>();
                    nvps.add(new BasicNameValuePair(SIGN, pwd));
                    nvps.add(new BasicNameValuePair(PRODUCTKEY, productKey));

                    //生成URI
                    URI uri = doGetHttpUrl(DEVICEHIVE_URL, requsetPath, nvps);

                    //Http Get请求dh
                    String response = sendGet(uri);
                    LOG.info("dh url :{},response from dh:{}", uri, response);
                    return dhResponseParser(response, clientId, deviceGuid, pwd);
                }
            } else if (clientId.contains(APP)) {
                LOG.info("client ask dh to login:" + clientId);
                //TODO:app端暂无验证接口
                return true;
            }
        }
        return false;
    }

    private boolean dhResponseParser(String response, String clientId, String deviceGuid, String password) {
        JsonObject jsonObject = new JsonParser().parse(response).getAsJsonObject();
        if (jsonObject.get(STATUS).getAsString().equals(SUCC)) {
            LOG.info("login success, client is:" + clientId);
            //记录已登录的clientId和登录时间
            redissonClient.getBucket(REDIS_PASSWORD_STORE + clientId).set(password);
            redissonClient.getBucket(REDIS_PASSWORD_STORE + clientId).expire(7, TimeUnit.DAYS);
            return true;
        } else if (jsonObject.get(STATUS).getAsString().equals(ERR)) {
            //日志根据dh返回值确定错误类型
            if (jsonObject.get(ERRORCODE).getAsLong() == USER_NOT_FOUND) {
                LOG.error("device with guid : {} not found", deviceGuid);
            } else if (jsonObject.get(ERRORCODE).getAsLong() == PASSWORD_INVALID) {
                LOG.error("device check valid, pwd is:", password);
            }
        }
        return false;
    }

}

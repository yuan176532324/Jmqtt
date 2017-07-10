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

package com.bigbigcloud.interception.messages;

public class InterceptDisconnectMessage implements InterceptMessage {

    private  String clientID;
    private  String username;
    private  String ip;

    public InterceptDisconnectMessage(String clientID, String username, String ip) {
        this.clientID = clientID;
        this.username = username;
        this.ip = ip;
    }

    public String getClientID() {
        return clientID;
    }

    public String getUsername() {
        return username;
    }

    public String getIp() {
        return ip;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}

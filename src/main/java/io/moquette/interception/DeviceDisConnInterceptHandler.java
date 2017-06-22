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

import com.hazelcast.core.HazelcastInstance;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceDisConnInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceDisConnInterceptHandler.class);
    private final HazelcastInstance hz;

    public DeviceDisConnInterceptHandler(Server server) {
        this.hz = server.getHazelcastInstance();
    }


    @Override
    public String getID() {
        return null;
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {

    }



}

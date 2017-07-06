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

package com.bigibgcloud.server;

import java.io.IOException;

import com.bigibgcloud.server.config.IConfig;
import com.bigibgcloud.spi.impl.ProtocolProcessor;
import com.bigibgcloud.spi.security.ISslContextCreator;

public interface ServerAcceptor {

    void initialize(ProtocolProcessor processor, IConfig props, ISslContextCreator sslCtxCreator) throws IOException;

    void close();
}

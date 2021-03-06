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
package com.bigbigcloud.persistence;
import java.io.Serializable;

/**
 * This is a DTO used to persist minimal status (clean session and activation status) of a
 * session.
 */
public class PersistentSession implements Serializable {

    private static final long serialVersionUID = 5052054783220481854L;
    private boolean cleanSession;
    private boolean active;
    private long timestamp;


    public PersistentSession() {
    }

    public PersistentSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        this.active = true;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

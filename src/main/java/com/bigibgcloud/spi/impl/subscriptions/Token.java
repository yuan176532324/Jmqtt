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

package com.bigibgcloud.spi.impl.subscriptions;

/**
 * Internal use only class.
 */
public class Token {

    public static final Token EMPTY = new Token("");
    public static final Token MULTI = new Token("#");
    public static final Token SINGLE = new Token("+");
    final String name;

    public Token(String s) {
        name = s;
    }

    public String name() {
        return name;
    }

    protected boolean match(Token t) {
        if (t == SINGLE) {
            return true;
        }
        if (t == MULTI) {
            return false;
        }
        if (this == MULTI || this == SINGLE) {
            return true;
        }

        return equals(t);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.name != null ? this.name.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Token other = (Token) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return name;
    }
}

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

import com.bigibgcloud.persistence.redis.RedissonUtil;
import com.bigibgcloud.spi.ISubscriptionsStore;
import org.redisson.api.RedissonClient;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class TreeNode {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TreeNode.class);
    public static final String TREE_NODE_KEY = "tn:";
    public static final String SUBSCRIPTIONS_KEY = "cli:";
    private Token m_token;
    private RedissonClient redissonClient;
    private String key = "";

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public TreeNode(){
    }

    public TreeNode(String key) throws IOException {
        this.redissonClient = RedissonUtil.getRedisson();
        this.key = key;
        this.m_token = new Token(getLastToken(key));
    }

    Token getToken() {
        return m_token;
    }

    void setToken(Token topic) {
        this.m_token = topic;
    }

    void addSubscription(ISubscriptionsStore.ClientTopicCouple s) {
        retriveSubscriptions(key).add(s.clientID);
    }

    void addChild(TreeNode child) {
        retrieveChildren(key).add(TREE_NODE_KEY + child.key);
    }

    /**
     * Creates a shallow copy of the current node. Copy the token and the children.
     */
    TreeNode copy() throws IOException {
        final TreeNode copy = new TreeNode();
        copy.key = key;
        copy.m_token = m_token;
        return copy;
    }

    /**
     * Search for children that has the specified token, if not found return null;
     */
    TreeNode childWithToken(Token token) throws IOException {
        return null;
    }

    void updateChild(TreeNode oldChild, TreeNode newChild) {
        retrieveChildren(key).remove(TREE_NODE_KEY + oldChild.key);
        retrieveChildren(key).add(TREE_NODE_KEY + newChild.key);
    }


    public void remove(ISubscriptionsStore.ClientTopicCouple clientTopicCouple) {
        retriveSubscriptions(key).remove(clientTopicCouple.clientID);
    }

    void matches(Queue<Token> tokens, List<ISubscriptionsStore.ClientTopicCouple> matchingSubs) throws IOException {
        Token t = tokens.poll();
        LOG.info("begin to match!!!" + t);
        // check if t is null <=> tokens finished
        if (t == null) {
            matchingSubs.addAll(getSubscriptions(key));
            // check if it has got a MULTI child and add its subscriptions
            for (String childrenKey : retrieveChildren(key)) {
                childrenKey = childrenKey.replace("tn:", "");
                TreeNode n = new TreeNode(childrenKey);
                if (n.getToken() == Token.MULTI || n.getToken() == Token.SINGLE) {
                    matchingSubs.addAll(n.getSubscriptions(childrenKey));
                }
                n.shutdown();
            }
            return;
        }
        // we are on MULTI, than add subscriptions and return
        if (m_token == Token.MULTI) {
            LOG.info("Here is  #!!");
            matchingSubs.addAll(getSubscriptions(key));
            return;
        }

        for (String childrenKey : retrieveChildren(key)) {
            childrenKey = childrenKey.replace("tn:", "");
            TreeNode n = new TreeNode(childrenKey);
            LOG.info("childrenKey :" + childrenKey + " ,Token is:" + n.getToken().name());
            if (n.getToken().equals(t) || n.getToken().equals(Token.SINGLE)) {
                LOG.info("match success!");
                // Create a copy of token, else if navigate 2 sibling it
                // consumes 2 elements on the queue instead of one
                n.matches(new LinkedBlockingQueue<>(tokens), matchingSubs);
                // TODO don't create a copy n.matches(tokens, matchingSubs);
            } else if (n.getToken().equals(Token.MULTI)) {
                matchingSubs.addAll(getSubscriptions(childrenKey));
            }

        }
    }

    private void shutdown() {
        RedissonUtil.closeRedisson(redissonClient);
    }

    private String getLastToken(String key) {
        if (key.contains("tn:") || key.contains("cli:")) {
            key = key.substring(3, key.length());
        }
        int index = key.lastIndexOf('/');
        if (index < 0) {
            return key;
        }
        return key.substring(index + 1, key.length());
    }

    private Collection<String> retrieveChildren(String key) {
        Collection<String> collection = redissonClient.getSet(TREE_NODE_KEY + key);
        shutdown();
        return collection;
    }

    private Collection<String> retriveSubscriptions(String key) {
        Collection<String> collection = redissonClient.getSet(SUBSCRIPTIONS_KEY + key);
        shutdown();
        return collection;
    }

    private Collection<ISubscriptionsStore.ClientTopicCouple> getSubscriptions(String key) {
        Collection<String> collection = redissonClient.getSet(SUBSCRIPTIONS_KEY + key);
        Collection<ISubscriptionsStore.ClientTopicCouple> clientTopicCouples = new HashSet<>();
        for (String s : collection) {
            Topic topic = new Topic(key);
            ISubscriptionsStore.ClientTopicCouple clientTopicCouple = new ISubscriptionsStore.ClientTopicCouple(s, topic);
            clientTopicCouples.add(clientTopicCouple);
        }
        shutdown();
        return clientTopicCouples;
    }

    public boolean compareAndSet(TreeNode oldRoot, TreeNode root) {
        return false;
    }
}

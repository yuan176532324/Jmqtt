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

import com.bigibgcloud.persistence.redis.RedisTreeNodeStore;
import com.bigibgcloud.spi.ISessionsStore;
import com.bigibgcloud.spi.ISubscriptionsStore;
import com.bigibgcloud.spi.ISubscriptionsStore.ClientTopicCouple;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a tree of topics subscriptions.
 */
public class SubscriptionsDirectory {
    private RedissonClient redissonClient;

    public SubscriptionsDirectory(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public static class NodeCouple {

        final RedisTreeNodeStore root;
        final RedisTreeNodeStore createdNode;

        public NodeCouple(RedisTreeNodeStore root, RedisTreeNodeStore createdNode) {
            this.root = root;
            this.createdNode = createdNode;
        }
    }

    private final AtomicReference<RedisTreeNodeStore> subscriptions = new AtomicReference<>(new RedisTreeNodeStore(redissonClient));
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionsDirectory.class);
    private volatile ISessionsStore m_sessionsStore;
    private volatile ISubscriptionsStore subscriptionsStore;

    /**
     * Initialize the subscription tree with the list of subscriptions. Maintained for compatibility
     * reasons.
     *
     * @param sessionsStore to be used as backing store from the subscription store.
     */
    public void init(ISessionsStore sessionsStore) throws IOException {
        LOG.info("Initializing subscriptions store...");
        m_sessionsStore = sessionsStore;
        this.subscriptionsStore = sessionsStore.subscriptionStore();
        List<ClientTopicCouple> subscriptions = this.subscriptionsStore.listAllSubscriptions();
        // reload any subscriptions persisted
//        if (LOG.isTraceEnabled()) {
//            LOG.trace("Reloading all stored subscriptions. SubscriptionTree = {}", dumpTree());
//        }

        for (ClientTopicCouple clientTopic : subscriptions) {
            LOG.info("Re-subscribing client to topic CId={}, topicFilter={}", clientTopic.clientID,
                    clientTopic.topicFilter);
            add(clientTopic);
        }
//        if (LOG.isTraceEnabled()) {
//            LOG.trace("Stored subscriptions have been reloaded. SubscriptionTree = {}", dumpTree());
//        }
    }

    public void add(ClientTopicCouple newSubscription) {
        /*
         * The topic filters have already been validated at the ProtocolProcessor. We can assume
         * they are valid.
         */
        RedisTreeNodeStore oldRoot;
        NodeCouple couple;
        do {
            oldRoot = subscriptions.get();
            couple = recreatePath(newSubscription.topicFilter, oldRoot);
            couple.createdNode.addSubscription(newSubscription); //createdNode could be null?
//            couple.root.recalculateSubscriptionsSize();
            //spin lock repeating till we can, swap root, if can't swap just re-do the operation
        } while (!subscriptions.compareAndSet(oldRoot, couple.root));
        LOG.debug("A subscription has been added. Root = {}, oldRoot = {}.", couple.root, oldRoot);
    }

    private NodeCouple recreatePath(Topic topic, final RedisTreeNodeStore oldRoot) {
        final RedisTreeNodeStore newRoot = new RedisTreeNodeStore(redissonClient);
        RedisTreeNodeStore parent = newRoot;
        RedisTreeNodeStore current = newRoot;
        AtomicReference<StringBuilder> stringBuilder = new AtomicReference<>(new StringBuilder());
        for (Token token : topic.getTokens()) {
            RedisTreeNodeStore matchingChildren;
            stringBuilder.get().append(token);
            // create a new node for the newly inserted token
            matchingChildren = new RedisTreeNodeStore(redissonClient);
            matchingChildren.setKey(stringBuilder.get().toString());
            matchingChildren.setToken(token);
            LOG.info(matchingChildren.getKey());
            current.addChild(matchingChildren);
            current = matchingChildren;
            stringBuilder.get().append("/");
        }
        return new NodeCouple(newRoot, current);
    }

    public void removeSubscription(Topic topic, String clientID) throws IOException {
        /*
         * The topic filters have already been validated at the ProtocolProcessor. We can assume
         * they are valid.
         */
        RedisTreeNodeStore oldRoot;
        NodeCouple couple;
        do {
            oldRoot = subscriptions.get();
            couple = recreatePath(topic, oldRoot);

            couple.createdNode.remove(new ClientTopicCouple(clientID, topic));
//            couple.root.recalculateSubscriptionsSize();
            //spin lock repeating till we can, swap root, if can't swap just re-do the operation
        } while (!subscriptions.compareAndSet(oldRoot, couple.root));
    }

    /**
     * Visit the topics tree to remove matching subscriptions with clientID. It's a mutating
     * structure operation so create a new subscription tree (partial or total).
     *
     * @param clientID the client ID to remove.
     */
//    public void removeForClient(String clientID) {
//        TreeNode oldRoot;
//        TreeNode newRoot;
//        do {
//            oldRoot = subscriptions.get();
//            newRoot = oldRoot.removeClientSubscriptions(clientID);
//            // spin lock repeating till we can, swap root, if can't swap just re-do the operation
//        } while (!subscriptions.compareAndSet(oldRoot, newRoot));
//    }

    /**
     * Given a topic string return the clients subscriptions that matches it. Topic string can't
     * contain character # and + because they are reserved to listeners subscriptions, and not topic
     * publishing.
     *
     * @param topic to use fo searching matching subscriptions.
     * @return the list of matching subscriptions, or empty if not matching.
     */
    public List<Subscription> matches(Topic topic) throws IOException {
        Queue<Token> tokenQueue = new LinkedBlockingDeque<>(topic.getTokens());
        List<ClientTopicCouple> matchingSubs = new ArrayList<>();
        subscriptions.get().matches(tokenQueue, matchingSubs);
        // remove the overlapping subscriptions, selecting ones with greatest qos
        Map<String, Subscription> subsForClient = new HashMap<>();
        for (ClientTopicCouple matchingCouple : matchingSubs) {
            Subscription existingSub = subsForClient.get(matchingCouple.clientID);
            Subscription sub = subscriptionsStore.getSubscription(matchingCouple);
            if (sub == null) {
                // if the m_sessionStore hasn't the sub because the client disconnected
                continue;
            }
            // update the selected subscriptions if not present or if has a greater qos
            if (existingSub == null || existingSub.getRequestedQos().value() < sub.getRequestedQos().value()) {
                subsForClient.put(matchingCouple.clientID, sub);
            }
        }
        return new ArrayList<>(subsForClient.values());
    }

    public boolean contains(Subscription sub) throws IOException {
        return !matches(sub.getTopicFilter()).isEmpty();
    }

//    public int size() {
//        return subscriptions.get().size();
//    }

//    public String dumpTree() {
//        DumpTreeVisitor visitor = new DumpTreeVisitor();
//        bfsVisit(subscriptions.get(), visitor, 0);
//        return visitor.getResult();
//    }

//    private void bfsVisit(TreeNode node, IVisitor<?> visitor, int deep) {
//        if (node == null) {
//            return;
//        }
//        visitor.visit(node, deep);
//        for (TreeNode child : node.m_children) {
//            bfsVisit(child, visitor, ++deep);
//        }
//    }
}

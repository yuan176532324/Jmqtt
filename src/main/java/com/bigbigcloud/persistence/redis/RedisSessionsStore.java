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

package com.bigbigcloud.persistence.redis;

import com.bigbigcloud.common.model.StoredMessage;
import com.bigbigcloud.spi.ISessionsStore;
import com.bigbigcloud.persistence.PersistentSession;
import com.bigbigcloud.spi.ClientSession;
import com.bigbigcloud.spi.ISubscriptionsStore;
import com.bigbigcloud.spi.impl.subscriptions.Subscription;
import com.bigbigcloud.spi.impl.subscriptions.Topic;
import org.redisson.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.bigbigcloud.BrokerConstants.*;

/**
 * ISessionsStore implementation backed by MapDB.
 */
public class RedisSessionsStore implements ISessionsStore, ISubscriptionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSessionsStore.class);

    private final RedissonClient redis;
    //订阅关系
    private RMap<Topic, Subscription> subMap;
    //persistSession
    private RMap<String, PersistentSession> persistSession;
    //第二阶段的消息
    private RMap<Integer, StoredMessage> secondPhaseMessages;
    //全部的sessionKeys
    private Collection<String> keyForSessions;

    public RedisSessionsStore(RedissonClient redis) {
        this.redis = redis;
    }

    @Override
    public void initStore() {
    }

    @Override
    public ISubscriptionsStore subscriptionStore() {
        return this;
    }

    @Override
    public void addNewSubscription(Subscription newSubscription) {
        LOG.info("Adding new subscription. ClientId={}, topics={}", newSubscription.getClientId(),
                newSubscription.getTopicFilter());
        final String clientID = newSubscription.getClientId();
        subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
        subMap.put(newSubscription.getTopicFilter(), newSubscription);
        subMap.expire(7, TimeUnit.DAYS);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Subscription has been added. ClientId={}, topics={}, clientSubscriptions={}",
                    newSubscription.getClientId(), newSubscription.getTopicFilter(),
                    redis.getMap("KEY_SUBSCRIPTIONS" + clientID));
        }
    }

    @Override
    public void removeSubscription(Topic topicFilter, String clientID) {
        LOG.info("Removing subscription. ClientId={}, topics={}", clientID, topicFilter);
        subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
        if (!subMap.isExists()) {
            return;
        }
        subMap.remove(topicFilter);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subscription has been removed. ClientId={}, topics={}, clientSubscriptions={}", clientID,
                    topicFilter, subMap);
        }
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        LOG.info("Wiping subscriptions. CId={}", clientID);
        subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
        subMap.delete();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subscriptions have been removed. ClientId={}, clientSubscriptions={}", clientID, subMap);
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        LOG.debug("Retrieving existing subscriptions");
        final List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        keyForSessions = redis.getKeys().findKeysByPattern(SESSION + "*");
        for (String key : keyForSessions) {
            String clientID = key.replace(SESSION, "");
            subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
            for (Topic topicFilter : subMap.keySet()) {
                allSubscriptions.add(new ClientTopicCouple(clientID, topicFilter));
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("The existing subscriptions have been retrieved. Result={}", allSubscriptions);
        }
        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {
        subMap = redis.getMap(KEY_SUBSCRIPTIONS + couple.clientID);
        LOG.debug("Retrieving subscriptions. CId={}, subscriptions={}", couple.clientID, subMap);
        return subMap.get(couple.topicFilter);
    }

    @Override
    public List<Subscription> getSubscriptions() {
        LOG.debug("Retrieving existing subscriptions...");
        List<Subscription> subscriptions = new ArrayList<>();
        keyForSessions = redis.getKeys().findKeysByPattern(SESSION + "*");
        for (String key : keyForSessions) {
            String clientID = key.replace(SESSION, "");
            subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
            subscriptions.addAll(subMap.values());
        }
        LOG.debug("Existing subscriptions has been retrieved Result={}", subscriptions);
        return subscriptions;
    }

    @Override
    public boolean contains(String clientID) {
        subMap = redis.getMap(KEY_SUBSCRIPTIONS + clientID);
        return subMap.isExists();
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        persistSession = redis.getMap(SESSION + clientID);
        if (persistSession.isExists()) {
            LOG.warn("Unable to create a new session: the client ID is already in use. ClientId={}, cleanSession={}",
                    clientID, cleanSession);
        }
        LOG.debug("Creating new session. CId={}, cleanSession={}", clientID, cleanSession);
        PersistentSession persistentSession = new PersistentSession(cleanSession);
        persistSession = redis.getMap(SESSION + clientID);
        persistSession.putIfAbsent(clientID, persistentSession);
        persistSession.expire(7, TimeUnit.DAYS);
        return new ClientSession(persistentSession.isActive(), clientID, this, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        LOG.debug("Retrieving session CId={}", clientID);
        persistSession = redis.getMap(SESSION + clientID);
        if (!persistSession.isExists()) {
            LOG.warn("Session does not exist CId={}", clientID);
            return null;
        }
        PersistentSession storedSession = persistSession.get(clientID);
        return new ClientSession(storedSession.isActive(), clientID, this, this, storedSession.isCleanSession());
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        keyForSessions = redis.getKeys().findKeysByPattern(SESSION + "*");
        for (String key : keyForSessions) {
            String clientID = key.replace(SESSION, "");
            persistSession = redis.getMap(key);
            PersistentSession entry = persistSession.get(clientID);
            result.add(new ClientSession(entry.isActive(), clientID, this, this, entry.isCleanSession()));
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        LOG.info("Updating cleanSession flag. CId={}, cleanSession={}", clientID, cleanSession);
        persistSession = redis.getMap(SESSION + clientID);
        if (!cleanSession) {
            persistSession.expire(7, TimeUnit.DAYS);
        }
        persistSession.put(clientID, new PersistentSession(cleanSession));
    }

    @Override
    public boolean isInBlackList(String clientID) {
        LOG.info("Check clientId is in blackList. CId={}", clientID);
        return redis.getList(BLACKLIST + clientID).isExists();
    }

    @Override
    public void putInBlackList(String clientID) {
        LOG.info("Put clientId is in blackList. CId={}", clientID);
        redis.getList(BLACKLIST + clientID).add(new Date().getTime());
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        LOG.debug("Generating next packet ID CId={}", clientID);
        RScoredSortedSet<Integer> inFlightForClient = redis.getScoredSortedSet(INFLIGHT_PACKETIDS + clientID);
        int maxId = inFlightForClient.isEmpty() ? 0 : inFlightForClient.last();
        int nextPacketId = (maxId % 0xFFFF) + 1;
        inFlightForClient.add(new Date().getTime(), nextPacketId);
        LOG.debug("Next packet ID has been generated CId={}, result={}", clientID, nextPacketId);
        return nextPacketId;
    }

    @Override
    public StoredMessage inFlightAck(String clientID, int messageID) {
        LOG.debug("Acknowledging inflight message CId={}, messageId={}", clientID, messageID);
        RBucket<StoredMessage> rStore = redis.getBucket(IN_FLIGHT + clientID + "_" + messageID);
        if (!rStore.isExists()) {
            LOG.warn("Can't find the inFlight record for client <{}>", clientID);
            return null;
        }
        StoredMessage msg = rStore.get();
        rStore.delete();
        // remove from the ids store
        RScoredSortedSet<Integer> inFlightForClient = redis.getScoredSortedSet(INFLIGHT_PACKETIDS + clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }
        return msg;
    }

    @Override
    public void inFlight(String clientID, int messageID, StoredMessage msg) {
        RBucket<TrackedMessage> rBucket = redis.getBucket(MESSAGE_STATUS + clientID + "_" + msg.getGuid().toString());
        rBucket.set(new TrackedMessage(MessageStatus.SENT_FIR), 1, TimeUnit.DAYS);
        RBucket<StoredMessage> rStore = redis.getBucket(IN_FLIGHT + clientID + "_" + messageID);
        rStore.set(msg, 1, TimeUnit.HOURS);
    }

    @Override
    public BlockingQueue<StoredMessage> queue(String clientID) {
        LOG.info("Queuing pending message. ClientId={}, guid={}", clientID);
        return this.redis.getBlockingQueue(clientID);
    }

    @Override
    public void dropQueue(String clientID) {
        LOG.info("Removing pending messages. ClientId={}", clientID);
        this.redis.getBlockingQueue(clientID).delete();
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID, StoredMessage msg) {
        LOG.debug("Moving inflight message to 2nd phase ack state. ClientId={}, messageID={}", clientID, messageID);
        RBucket<TrackedMessage> rBucket = redis.getBucket(MESSAGE_STATUS + clientID + "_" + msg.getGuid().toString());
        rBucket.set(new TrackedMessage(MessageStatus.SENT_SEC), 1, TimeUnit.DAYS);
        RBucket<StoredMessage> rStore = redis.getBucket(SECOND_FLIGHT + clientID + "_" + msg.getGuid().toString());
        rStore.set(msg, 1, TimeUnit.DAYS);
    }

    @Override
    public StoredMessage secondPhaseAcknowledged(String clientID, int messageID) {
        LOG.debug("Processing second phase ACK CId={}, messageId={}", clientID, messageID);
        RBucket<StoredMessage> rStore = redis.getBucket(SECOND_FLIGHT + clientID + "_" + messageID);
        if (!rStore.isExists()) {
            String error = String.format("Can't find the inFlight record for client <%s> during the second phase " +
                    "acking of QoS2 pub", clientID);
            LOG.warn(error);
        }
        StoredMessage storedMessage = rStore.get();
        rStore.delete();
        return storedMessage;
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        return 0;
    }

    @Override
    public StoredMessage inboundInflight(String clientID, int messageID) {
        LOG.debug("Mapping message ID to GUID CId={}, messageId={}", clientID, messageID);
        RBucket<StoredMessage> rStore = redis.getBucket(IN_FLIGHT + clientID + "_" + messageID);
        return rStore.get();
    }

    @Override
    public void markAsInboundInflight(String clientID, int messageID, StoredMessage msg) {
        RBucket<TrackedMessage> rBucket = redis.getBucket(MESSAGE_STATUS + clientID + "_" + msg.getGuid().toString());
        rBucket.set(new TrackedMessage(MessageStatus.SENT_FIR), 1, TimeUnit.DAYS);
        RBucket<StoredMessage> rStore = redis.getBucket(IN_FLIGHT + clientID + "_" + messageID);
        rStore.set(msg, 1, TimeUnit.DAYS);
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        return queue(clientID).size();
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        if (!redis.getMap(SECOND_PHASE + clientID).isExists())
            return 0;
        return redis.getMap(SECOND_PHASE + clientID).size();
    }

    @Override
    public void offlineSession(String clientID) {
        if (!redis.getMap(SESSION + clientID).isExists()) {
            LOG.warn("Session does not exist CId={}", clientID);
            return;
        }
        persistSession = redis.getMap(SESSION + clientID);
        PersistentSession persistentSession = persistSession.get(clientID);
        persistentSession.setActive(false);
        LOG.info("set persistentSession:" + persistentSession.toString());
        persistSession.put(clientID, persistentSession);
    }

    @Override
    public boolean getSessionStatus(String clientID) {
        if (!redis.getMap(SESSION + clientID).isExists()) {
            LOG.warn("Session does not exist CId={}", clientID);
            return false;
        }
        persistSession = redis.getMap(SESSION + clientID);
        PersistentSession persistentSession = persistSession.get(clientID);
        return persistentSession.isActive();
    }

    @Override
    public void cleanSession(String clientID) {
        // remove also the messages stored of type QoS1/2
        LOG.info("Removing stored messages with QoS 1 and 2. ClientId={}", clientID);
        redis.getMap(SECOND_PHASE + clientID).delete();
        redis.getMap(OUTBOUND_FLIGHT + clientID).delete();
        redis.getSet(INFLIGHT_PACKETIDS + clientID).delete();

        LOG.info("Wiping existing subscriptions. ClientId={}", clientID);
        wipeSubscriptions(clientID);

        //remove also the enqueued messages
        dropQueue(clientID);
    }


    private static String inboundMessageId2MessagesMapName(String clientID) {
        return INBOUND_INFLIGHT + clientID;
    }
}

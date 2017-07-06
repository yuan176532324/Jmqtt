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

package com.bigibgcloud.persistence.redis;

import com.bigbigcloud.common.model.StoredMessage;
import com.bigibgcloud.persistence.PersistentSession;
import com.bigibgcloud.spi.ClientSession;
import com.bigibgcloud.spi.ISessionsStore;
import com.bigibgcloud.spi.ISubscriptionsStore;
import com.bigibgcloud.spi.impl.subscriptions.Subscription;
import com.bigibgcloud.spi.impl.subscriptions.Topic;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * ISessionsStore implementation backed by MapDB.
 */
public class RedisSessionsStore implements ISessionsStore, ISubscriptionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSessionsStore.class);

    // maps clientID->[MessageId -> msg]
    private RMap<String, ConcurrentMap<Integer, StoredMessage>> outboundFlightMessages;
    // map clientID <-> set of currently in flight packet identifiers
    private RMap<String, Set<Integer>> m_inFlightIds;
    private RMap<String, PersistentSession> m_persistentSessions;
    // maps clientID->[MessageId -> guid]
    private RMap<String, ConcurrentMap<Integer, StoredMessage>> m_secondPhaseStore;
    private RMap<String, String> m_blacklist;
    private final RedissonClient m_db;

    public RedisSessionsStore(RedissonClient db) {
        m_db = db;
    }

    @Override
    public void initStore() {
        outboundFlightMessages = m_db.getMap("outboundFlight");
        m_inFlightIds = m_db.getMap("inflightPacketIDs");
        m_persistentSessions = m_db.getMap("sessions");
        m_secondPhaseStore = m_db.getMap("secondPhase");
        m_blacklist = m_db.getMap("blackList");
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
        m_db.getMap("subscriptions_" + clientID).put(newSubscription.getTopicFilter(), newSubscription);
        m_db.getMap("subscriptions_" + clientID).expire(7, TimeUnit.DAYS);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Subscription has been added. ClientId={}, topics={}, clientSubscriptions={}",
                    newSubscription.getClientId(), newSubscription.getTopicFilter(),
                    m_db.getMap("subscriptions_" + clientID));
        }
    }

    @Override
    public void removeSubscription(Topic topicFilter, String clientID) {
        LOG.info("Removing subscription. ClientId={}, topics={}", clientID, topicFilter);
        if (!m_db.getMap("subscriptions_" + clientID).isExists()) {
            return;
        }
        m_db.getMap("subscriptions_" + clientID).remove(topicFilter);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subscription has been removed. ClientId={}, topics={}, clientSubscriptions={}", clientID,
                    topicFilter, m_db.getMap("subscriptions_" + clientID));
        }
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        LOG.info("Wiping subscriptions. CId={}", clientID);
        m_db.getMap("subscriptions_" + clientID).delete();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subscriptions have been removed. ClientId={}, clientSubscriptions={}", clientID,
                    m_db.getMap("subscriptions_" + clientID));
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        LOG.debug("Retrieving existing subscriptions");
        final List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        for (String clientID : m_persistentSessions.keySet()) {
            ConcurrentMap<Topic, Subscription> clientSubscriptions = m_db.getMap("subscriptions_" + clientID);
            for (Topic topicFilter : clientSubscriptions.keySet()) {
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
        ConcurrentMap<Topic, Subscription> clientSubscriptions = m_db.getMap("subscriptions_" + couple.clientID);
        LOG.debug("Retrieving subscriptions. CId={}, subscriptions={}", couple.clientID, clientSubscriptions);
        return clientSubscriptions.get(couple.topicFilter);
    }

    @Override
    public List<Subscription> getSubscriptions() {
        LOG.debug("Retrieving existing subscriptions...");
        List<Subscription> subscriptions = new ArrayList<>();
        for (String clientID : m_persistentSessions.keySet()) {
            ConcurrentMap<Topic, Subscription> clientSubscriptions = m_db.getMap("subscriptions_" + clientID);
            subscriptions.addAll(clientSubscriptions.values());
        }
        LOG.debug("Existing subscriptions has been retrieved Result={}", subscriptions);
        return subscriptions;
    }

    @Override
    public boolean contains(String clientID) {
        return m_db.getMap("subscriptions_" + clientID).isExists();
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        if (m_persistentSessions.containsKey(clientID)) {
            LOG.error("Unable to create a new session: the client ID is already in use. ClientId={}, cleanSession={}",
                    clientID, cleanSession);
            throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);
        }
        LOG.debug("Creating new session. CId={}, cleanSession={}", clientID, cleanSession);
        PersistentSession persistentSession = new PersistentSession(cleanSession);
        m_persistentSessions.putIfAbsent(clientID, persistentSession);
        m_persistentSessions.expire(7, TimeUnit.DAYS);
        return new ClientSession(persistentSession.isActive(), clientID, this, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        LOG.debug("Retrieving session CId={}", clientID);
        if (!m_persistentSessions.containsKey(clientID)) {
            LOG.warn("Session does not exist CId={}", clientID);
            return null;
        }

        PersistentSession storedSession = m_persistentSessions.get(clientID);
        return new ClientSession(storedSession.isActive(), clientID, this, this, storedSession.isCleanSession());
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        for (Map.Entry<String, PersistentSession> entry : m_persistentSessions.entrySet()) {
            result.add(new ClientSession(entry.getValue().isActive(), entry.getKey(), this, this, entry.getValue().isCleanSession()));
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        LOG.info("Updating cleanSession flag. CId={}, cleanSession={}", clientID, cleanSession);
        m_persistentSessions.put(clientID, new PersistentSession(cleanSession));
    }

    @Override
    public boolean isInBlackList(String clientID) {
        LOG.info("Check clientId is in blackList. CId={}", clientID);
        return m_blacklist.containsKey("bl:" + clientID);
    }

    @Override
    public void putInBlackList(String clientID) {
        LOG.info("Put clientId is in blackList. CId={}", clientID);
        m_blacklist.putIfAbsent("bl:" + clientID, "");
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        LOG.debug("Generating next packet ID CId={}", clientID);
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient == null) {
            int nextPacketId = 1;
            inFlightForClient = new HashSet<>();
            inFlightForClient.add(nextPacketId);
            this.m_inFlightIds.put(clientID, inFlightForClient);
            return nextPacketId;

        }

        int maxId = inFlightForClient.isEmpty() ? 0 : Collections.max(inFlightForClient);
        int nextPacketId = (maxId % 0xFFFF) + 1;
        inFlightForClient.add(nextPacketId);
        LOG.debug("Next packet ID has been generated CId={}, result={}", clientID, nextPacketId);
        return nextPacketId;
    }

    @Override
    public StoredMessage inFlightAck(String clientID, int messageID) {
        LOG.debug("Acknowledging inflight message CId={}, messageId={}", clientID, messageID);
        ConcurrentMap<Integer, StoredMessage> m = this.outboundFlightMessages.get(clientID);
        if (m == null) {
            LOG.error("Can't find the inFlight record for client <{}>", clientID);
            throw new RuntimeException("Can't find the inFlight record for client <" + clientID + ">");
        }
        LOG.info(m.toString());
        StoredMessage msg = m.remove(messageID);
        LOG.info(msg.toString());
        this.outboundFlightMessages.put(clientID, m);

        // remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }
        return msg;
    }

    @Override
    public void inFlight(String clientID, int messageID, StoredMessage msg) {
        ConcurrentMap<Integer, StoredMessage> messages = outboundFlightMessages.get(clientID);
        if (messages == null) {
            messages = new ConcurrentHashMap<>();
        }
        messages.put(messageID, msg);
        outboundFlightMessages.put(clientID, messages);
    }

    @Override
    public BlockingQueue<StoredMessage> queue(String clientID) {
        LOG.info("Queuing pending message. ClientId={}, guid={}", clientID);
        return this.m_db.getBlockingQueue(clientID);
    }

    @Override
    public void dropQueue(String clientID) {
        LOG.info("Removing pending messages. ClientId={}", clientID);
        this.m_db.getBlockingQueue(clientID).delete();
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID, StoredMessage msg) {
        LOG.debug("Moving inflight message to 2nd phase ack state. ClientId={}, messageID={}", clientID, messageID);
        ConcurrentMap<Integer, StoredMessage> m = this.m_secondPhaseStore.get(clientID);
        if (m == null) {
            String error = String.format("Can't find the inFlight record for client <%s> during the second phase of " +
                    "QoS2 pub", clientID);
            LOG.error(error);
            throw new RuntimeException(error);
        }
        m.put(messageID, msg);
        this.outboundFlightMessages.put(clientID, m);
    }

    @Override
    public StoredMessage secondPhaseAcknowledged(String clientID, int messageID) {
        LOG.debug("Processing second phase ACK CId={}, messageId={}", clientID, messageID);
        final ConcurrentMap<Integer, StoredMessage> m = this.m_secondPhaseStore.get(clientID);
        if (m == null) {
            String error = String.format("Can't find the inFlight record for client <%s> during the second phase " +
                    "acking of QoS2 pub", clientID);
            LOG.error(error);
            throw new RuntimeException(error);
        }

        StoredMessage msg = m.remove(messageID);
        m_secondPhaseStore.put(clientID, m);
        return msg;
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        int totalInflight = 0;
        ConcurrentMap<Integer, StoredMessage> inflightPerClient = m_db.getMap(inboundMessageId2MessagesMapName(clientID));
        if (inflightPerClient != null) {
            totalInflight += inflightPerClient.size();
        }

        Map<Integer, StoredMessage> secondPhaseInFlight = this.m_secondPhaseStore.get(clientID);
        if (secondPhaseInFlight != null) {
            totalInflight += secondPhaseInFlight.size();
        }

        Map<Integer, StoredMessage> outboundPerClient = outboundFlightMessages.get(clientID);
        if (outboundPerClient != null) {
            totalInflight += outboundPerClient.size();
        }

        return totalInflight;
    }

    @Override
    public StoredMessage inboundInflight(String clientID, int messageID) {
        LOG.debug("Mapping inbound message ID to GUID CId={}, messageId={}", clientID, messageID);
        ConcurrentMap<Integer, StoredMessage> messageIdToGuid = m_db.getMap(inboundMessageId2MessagesMapName(clientID));
        return messageIdToGuid.get(messageID);
    }

    @Override
    public void markAsInboundInflight(String clientID, int messageID, StoredMessage msg) {
        ConcurrentMap<Integer, StoredMessage> messageIdToGuid = m_db.getMap(inboundMessageId2MessagesMapName(clientID));
        messageIdToGuid.put(messageID, msg);
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        return queue(clientID).size();
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        if (!m_secondPhaseStore.containsKey(clientID))
            return 0;
        return m_secondPhaseStore.get(clientID).size();
    }

    @Override
    public void offlineSession(String clientID) {
        if (!m_persistentSessions.containsKey(clientID)) {
            LOG.warn("Session does not exist CId={}", clientID);
            return;
        }
        PersistentSession persistentSession = m_persistentSessions.get(clientID);
        persistentSession.setActive(false);
        m_persistentSessions.put(clientID, persistentSession);
    }

    @Override
    public boolean getSessionStatus(String clientID) {
        if (!m_persistentSessions.containsKey(clientID)) {
            LOG.warn("Session does not exist CId={}", clientID);
            return false;
        }
        PersistentSession persistentSession = m_persistentSessions.get(clientID);
        return persistentSession.isActive();
    }

    @Override
    public void cleanSession(String clientID) {
        // remove also the messages stored of type QoS1/2
        LOG.info("Removing stored messages with QoS 1 and 2. ClientId={}", clientID);
        m_secondPhaseStore.remove(clientID);
        outboundFlightMessages.remove(clientID);
        m_inFlightIds.remove(clientID);

        LOG.info("Wiping existing subscriptions. ClientId={}", clientID);
        wipeSubscriptions(clientID);

        //remove also the enqueued messages
        dropQueue(clientID);
    }


    private static String inboundMessageId2MessagesMapName(String clientID) {
        return "inboundInflight_" + clientID;
    }
}

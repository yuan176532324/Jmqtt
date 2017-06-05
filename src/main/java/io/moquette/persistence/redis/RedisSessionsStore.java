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

package io.moquette.persistence.redis;

import io.moquette.persistence.PersistentSession;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.Utils;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ISessionsStore implementation backed by MapDB.
 */
public class RedisSessionsStore implements ISessionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSessionsStore.class);
    // maps clientID->[MessageId -> guid]
    //ConcurrentMap<Integer, MessageGUID>
    private ConcurrentMap<String, ConcurrentMap<Integer, MessageGUID>> m_inflightStore;
    // map clientID <-> set of currently in flight packet identifiers
    //Set<Integer>
    private Map<String, Set<Integer>> m_inFlightIds;
    //PersistentSession
    private ConcurrentMap<String, PersistentSession> m_persistentSessions;
    // maps clientID->[MessageId -> guid]
    //Map<Integer, MessageGUID>
    private ConcurrentMap<String, Map<Integer, MessageGUID>> m_secondPhaseStore;

    private final RedissonClient m_db;
    private final IMessagesStore m_messagesStore;

    public RedisSessionsStore(RedissonClient db, IMessagesStore messagesStore) {
        m_db = db;
        m_messagesStore = messagesStore;
    }

    @Override
    public void initStore() {
        m_inflightStore = m_db.getMap("inflight");
        m_inFlightIds = m_db.getMap("inflightPacketIDs");
        m_persistentSessions = m_db.getMap("sessions");
        m_secondPhaseStore = m_db.getMap("secondPhase");
    }


    @Override
    public void addNewSubscription(Subscription newSubscription) {
        LOG.info("Adding new subscription. ClientId={}, topics={}", newSubscription.getClientId(),
            newSubscription.getTopicFilter());
        final String clientID = newSubscription.getClientId();
        m_db.getMap("subscriptions_" + clientID).put(newSubscription.getTopicFilter(), newSubscription);

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
        LOG.info("Wiping subscriptions. ClientId={}", clientID);
        m_db.getMap("subscriptions_" + clientID).clear();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subscriptions have been removed. ClientId={}, clientSubscriptions={}", clientID,
                m_db.getMap("subscriptions_" + clientID));
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
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
        LOG.info("Retrieving subscriptions. ClientId={}, subscriptions={}", couple.clientID, clientSubscriptions);
        return clientSubscriptions.get(couple.topicFilter);
    }

    @Override
    public List<Subscription> getSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
        List<Subscription> subscriptions = new ArrayList<>();
        for (String clientID : m_persistentSessions.keySet()) {
            ConcurrentMap<Topic, Subscription> clientSubscriptions = m_db.getMap("subscriptions_" + clientID);
            subscriptions.addAll(clientSubscriptions.values());
        }
        LOG.debug("The existing subscriptions have been retrieved. Result={}", subscriptions);
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
        LOG.info("Creating new session. ClientId={}, cleanSession={}", clientID, cleanSession);
        PersistentSession persistentSession = new PersistentSession();
        persistentSession.setCleanSession(cleanSession);
        m_persistentSessions.putIfAbsent(clientID, persistentSession);
        return new ClientSession(clientID, m_messagesStore, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        LOG.info("Retrieving session. ClientId={}", clientID);
        if (!m_persistentSessions.containsKey(clientID)) {
            LOG.warn("The session does not exist. ClientId={}", clientID);
            return null;
        }

        PersistentSession storedSession = m_persistentSessions.get(clientID);
        return new ClientSession(clientID, m_messagesStore, this, storedSession.isCleanSession());
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        for (Map.Entry<String, PersistentSession> entry : m_persistentSessions.entrySet()) {
            PersistentSession persistentSession = entry.getValue();
            result.add(new ClientSession(entry.getKey(), m_messagesStore, this, persistentSession.isCleanSession()));
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        LOG.info("Updating cleanSession flag. ClientId={}, cleanSession={}", clientID, cleanSession);
        PersistentSession persistentSession = new PersistentSession();
        persistentSession.setCleanSession(cleanSession);
        m_persistentSessions.put(clientID, persistentSession);
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating next packet ID. ClientId={}", clientID);
        }
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient == null) {
            int nextPacketId = 1;
            inFlightForClient = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
            inFlightForClient.add(nextPacketId);
            this.m_inFlightIds.put(clientID, inFlightForClient);
            return nextPacketId;

        }

        int maxId = inFlightForClient.isEmpty() ? 0 : Collections.max(inFlightForClient);
        int nextPacketId = (maxId % 0xFFFF) + 1;
        inFlightForClient.add(nextPacketId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("The next packet ID has been generated. ClientId={}, result={}", clientID, nextPacketId);
        }
        return nextPacketId;
    }

    @Override
    public void inFlightAck(String clientID, int messageID) {
        LOG.debug("Acknowledging inflight message. ClientId={}, messageId={}", clientID, messageID);
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.warn("Unable to retrieve inflight message record. ClientId={}, messageId={}", clientID, messageID);
            return;
        }
        m.remove(messageID);

        // remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }
    }

    @Override
    public void inFlight(String clientID, int messageID, MessageGUID guid) {
        LOG.debug("Storing inflight message. ClientId={}, messageId={}, guid={}", clientID, messageID, guid);
        ConcurrentMap<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            m = new ConcurrentHashMap<>();
        }
        m.put(messageID, guid);
        LOG.info("storing inflight clientID <{}> messageID {} guid <{}>", clientID, messageID, guid);
        this.m_inflightStore.put(clientID, m);
    }

    @Override
    public BlockingQueue<StoredMessage> queue(String clientID) {
        LOG.info("Queuing pending message. ClientId={}, guid={}", clientID);
        return this.m_db.getBlockingQueue(clientID);
    }

    @Override
    public void dropQueue(String clientID) {
        LOG.info("Removing pending messages. ClientId={}", clientID);
        this.m_db.getBlockingQueue(clientID).clear();
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Moving inflight message to 2nd phase ack state. ClientId={}, messageID={}", clientID, messageID);
        }
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.warn("Unable to retrieve inflight message record. ClientId={}, messageId={}", clientID, messageID);
            return;
        }
        MessageGUID guid = m.remove(messageID);

        // remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }

        final HashMap<Integer, MessageGUID> emptyGuids = new HashMap<>();
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, emptyGuids);
        messageIDs.put(messageID, guid);
        m_secondPhaseStore.put(clientID, messageIDs);
    }

    @Override
    public MessageGUID secondPhaseAcknowledged(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing second phase ACK CId={}, messageId={}", clientID, messageID);
        }
        final HashMap<Integer, MessageGUID> emptyGuids = new HashMap<>();
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, emptyGuids);
        MessageGUID guid = messageIDs.remove(messageID);
        m_secondPhaseStore.put(clientID, messageIDs);
        return guid;
    }

    @Override
    public StoredMessage getInflightMessage(String clientID, int messageID) {
        LOG.info("Retrieving inflight message CId={}, messageId={}", clientID, messageID);
        Map<Integer, MessageGUID> clientEntries = m_inflightStore.get(clientID);
        if (clientEntries == null) {
            LOG.warn("The client has no inflight messages CId={}, messageId={}", clientID, messageID);
            return null;
        }
        MessageGUID guid = clientEntries.get(messageID);
        if (guid == null) {
            LOG.warn("The message ID does not have an associated GUID. ClientId={}, messageId={}", clientID, messageID);
            return null;
        }
        return m_messagesStore.getMessageByGuid(guid);
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        if (!m_inflightStore.containsKey(clientID))
            return 0;
        else
            return m_inflightStore.get(clientID).size();
    }

    @Override
    public StoredMessage inboundInflight(String clientID, int messageID) {
        LOG.debug("Mapping inbound message ID to GUID CId={}, messageId={}", clientID, messageID);
        ConcurrentMap<Integer, MessageGUID> messageIdToGuid = m_db.getMap(inboundMessageId2GuidsMapName(clientID));
        final MessageGUID guid = messageIdToGuid.get(messageID);
        LOG.debug("Inbound message ID has been mapped CId={}, messageId={}, guid={}", clientID, messageID, guid);
        return m_messagesStore.getMessageByGuid(guid);
    }

    @Override
    public void markAsInboundInflight(String clientID, int messageID, MessageGUID guid) {
        ConcurrentMap<Integer, MessageGUID> messageIdToGuid = m_db.getMap(inboundMessageId2GuidsMapName(clientID));
        messageIdToGuid.put(messageID, guid);
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        return queue(clientID).size();
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        if (!m_secondPhaseStore.containsKey(clientID))
            return 0;
        else
            return m_secondPhaseStore.get(clientID).size();
    }

    @Override
    public Collection<MessageGUID> pendingAck(String clientID) {
        ConcurrentMap<Integer, MessageGUID> messageGUIDMap = m_db.getMap(messageId2GuidsMapName(clientID));
        if (messageGUIDMap == null || messageGUIDMap.isEmpty()) {
            return Collections.emptyList();
        }

        return new ArrayList<>(messageGUIDMap.values());
    }

    private static String messageId2GuidsMapName(String clientID) {
        return "guidsMapping_" + clientID;
    }

    private static String inboundMessageId2GuidsMapName(String clientID) {
        return "inboundInflightGuidsMapping_" + clientID;
    }
}

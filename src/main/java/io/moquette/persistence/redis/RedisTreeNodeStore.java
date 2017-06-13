package io.moquette.persistence.redis;

import io.moquette.spi.ISubscriptionsStore;
import io.moquette.spi.impl.subscriptions.Token;
import io.moquette.spi.impl.subscriptions.Topic;
import org.redisson.api.RedissonClient;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Administrator on 2017/6/12.
 */
public class RedisTreeNodeStore {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisTreeNodeStore.class);
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

    public RedisTreeNodeStore(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public RedisTreeNodeStore(RedissonClient redissonClient, String key) {
        this.redissonClient = redissonClient;
        this.key = key;
        this.m_token = new Token(getLastToken(key));
    }

    public Token getToken() {
        return m_token;
    }

    public void setToken(Token topic) {
        this.m_token = topic;
    }

    public void addSubscription(ISubscriptionsStore.ClientTopicCouple s) {
        retriveSubscriptions(key).add(s.clientID);
    }


    public void addChild(RedisTreeNodeStore child) {
        retrieveChildren(key).add(TREE_NODE_KEY + child.key);
    }

    /**
     * Creates a shallow copy of the current node. Copy the token and the children.
     */
    public RedisTreeNodeStore copy() {
        final RedisTreeNodeStore copy = new RedisTreeNodeStore(redissonClient);
        copy.key = key;
        copy.m_token = m_token;
        return copy;
    }

    /**
     * Search for children that has the specified token, if not found return null;
     */
    public RedisTreeNodeStore childWithToken(Token token) {
        return null;
    }


    public void remove(ISubscriptionsStore.ClientTopicCouple clientTopicCouple) {
        retriveSubscriptions(key).remove(clientTopicCouple.clientID);
    }

    public void matches(Queue<Token> tokens, List<ISubscriptionsStore.ClientTopicCouple> matchingSubs) {
        Token t = tokens.poll();
        LOG.info("begin to match!!!" + t);
        // check if t is null <=> tokens finished
        if (t == null) {
            matchingSubs.addAll(getSubscriptions(key));
            // check if it has got a MULTI child and add its subscriptions
            for (String childrenKey : retrieveChildren(key)) {
                childrenKey = childrenKey.replace("tn:", "");
                RedisTreeNodeStore n = new RedisTreeNodeStore(redissonClient, childrenKey);
                if (n.getToken() == Token.MULTI || n.getToken() == Token.SINGLE) {
                    matchingSubs.addAll(n.getSubscriptions(childrenKey));
                }
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
            RedisTreeNodeStore n = new RedisTreeNodeStore(redissonClient, childrenKey);
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
        return redissonClient.getSet(TREE_NODE_KEY + key);
    }

    private Collection<String> retriveSubscriptions(String key) {
        return redissonClient.getSet(SUBSCRIPTIONS_KEY + key);
    }

    private Collection<ISubscriptionsStore.ClientTopicCouple> getSubscriptions(String key) {
        Collection<String> collection = redissonClient.getSet(SUBSCRIPTIONS_KEY + key);
        Collection<ISubscriptionsStore.ClientTopicCouple> clientTopicCouples = new HashSet<>();
        for (String s : collection) {
            Topic topic = new Topic(key);
            ISubscriptionsStore.ClientTopicCouple clientTopicCouple = new ISubscriptionsStore.ClientTopicCouple(s, topic);
            clientTopicCouples.add(clientTopicCouple);
        }
        return clientTopicCouples;
    }


}



package io.moquette.server;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public abstract class MessageListener<T> extends Thread  {
    protected final Log logger = LogFactory.getLog(getClass());


    MessageListener() {
    }

    protected abstract T fromBytes(byte[] bytes) throws Exception;

    protected abstract void processMessageInternal(T payload) throws Exception;

}

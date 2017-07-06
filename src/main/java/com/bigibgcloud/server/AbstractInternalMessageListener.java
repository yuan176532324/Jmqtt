package com.bigibgcloud.server;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class AbstractInternalMessageListener<T> implements MessageListener {

    protected final Log logger = LogFactory.getLog(getClass());

    public Action consume(Message message, ConsumeContext context) {
        logger.info("Receive: " + messageInfo(message));
        try {
            T payload = fromBytes(message.getBody());
            if (payload != null) {
                processMessageInternal(payload, message, context);
            }
            return Action.CommitMessage;
        } catch (Exception e) {
            if (message.getReconsumeTimes() > 2) {
                logger.error("Consume failed: " + messageInfo(message), e);
            } else {
                logger.warn("Consume failed: " + messageInfo(message), e);
            }
            return Action.ReconsumeLater;
        }
    }

    private String messageInfo(Message message) {
        return message.getMsgID() + " Consume Times:" + message.getReconsumeTimes();
    }

    protected abstract T fromBytes(byte[] bytes) throws Exception;

    protected abstract void processMessageInternal(T payload, Message message, ConsumeContext context) throws Exception;

}

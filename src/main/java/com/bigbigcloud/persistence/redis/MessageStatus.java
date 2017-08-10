package com.bigbigcloud.persistence.redis;

/**
 * Created by Administrator on 2017/5/23.
 */


public enum MessageStatus {
    PUB_TO_MQ(0),
    READY_TO_PUB(1),
    PUB_TO_SUBER(2),
    SENT_FIR(3),
    SENT_SEC(4),
    PUB_OFFLINE(5),
    COMPLETED(6),
    SUCCESS(7),
    FAILURE(8);

    private final int value;

    MessageStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.name();
    }

    public static MessageStatus forName(String value) {
        for (MessageStatus type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }
}
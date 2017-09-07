package com.bigbigcloud.interception;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.Map;


public class KafkaMessageConverter implements Deserializer<KafkaMsg>, Serializer<KafkaMsg> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConverter.class);
    protected Gson gson = (new GsonBuilder()).disableHtmlEscaping().registerTypeAdapter(Date.class, new com.bigbigcloud.common.json.adapters.TimestampAdapter()).create();

    public KafkaMessageConverter() {
    }

    public byte[] toBytes(KafkaMsg obj) {
        return this.toJsonString(obj).getBytes();
    }

    public String toJsonString(KafkaMsg obj) {
        return this.gson.toJson(obj);
    }

    public KafkaMsg fromString(String string) {
        return this.gson.fromJson(string, KafkaMsg.class);
    }

    public KafkaMsg fromBytes(byte[] bytes) {
        try {
            return this.fromString(new String(bytes, "UTF-8"));
        } catch (Exception ex) {
            LOG.error("msg fromBytes error , ex is: {}", ex);
            return null;
        }
    }

    public KafkaMsg deserialize(String s, byte[] bytes) {
        return this.fromBytes(bytes);
    }

    public void configure(Map<String, ?> map, boolean b) {
    }

    public byte[] serialize(String s, KafkaMsg t) {
        return this.toBytes(t);
    }

    public void close() {
    }
}

package com.bigibgcloud.interception;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;


public class KafkaMessageConverter implements Deserializer<KafkaMsg>, Serializer<KafkaMsg> {
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
        } catch (UnsupportedEncodingException var3) {
            var3.printStackTrace();
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

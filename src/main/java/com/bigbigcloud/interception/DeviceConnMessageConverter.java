package com.bigbigcloud.interception;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;


public class DeviceConnMessageConverter implements Deserializer<DeviceConnMsg>, Serializer<DeviceConnMsg> {
    protected Gson gson = (new GsonBuilder()).disableHtmlEscaping().registerTypeAdapter(Date.class, new com.bigbigcloud.common.json.adapters.TimestampAdapter()).create();

    public DeviceConnMessageConverter() {
    }

    public byte[] toBytes(DeviceConnMsg obj) {
        return this.toJsonString(obj).getBytes();
    }

    public String toJsonString(DeviceConnMsg obj) {
        return this.gson.toJson(obj);
    }

    public DeviceConnMsg fromString(String string) {
        return this.gson.fromJson(string, DeviceConnMsg.class);
    }

    public DeviceConnMsg fromBytes(byte[] bytes) {
        try {
            return this.fromString(new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException var3) {
            var3.printStackTrace();
            return null;
        }
    }

    public DeviceConnMsg deserialize(String s, byte[] bytes) {
        return this.fromBytes(bytes);
    }

    public void configure(Map<String, ?> map, boolean b) {
    }

    public byte[] serialize(String s, DeviceConnMsg t) {
        return this.toBytes(t);
    }

    public void close() {
    }
}

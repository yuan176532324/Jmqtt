package io.moquette.spi.impl.security;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;


public class UserInfoMessageConverter implements Deserializer<UserInfo>, Serializer<UserInfo> {
    protected Gson gson = (new GsonBuilder()).disableHtmlEscaping().registerTypeAdapter(Date.class, new com.bigbigcloud.common.json.adapters.TimestampAdapter()).create();

    public UserInfoMessageConverter() {
    }

    public byte[] toBytes(UserInfo obj) {
        return this.toJsonString(obj).getBytes();
    }

    public String toJsonString(UserInfo obj) {
        return this.gson.toJson(obj);
    }

    public UserInfo fromString(String string) {
        return this.gson.fromJson(string, UserInfo.class);
    }

    public UserInfo fromBytes(byte[] bytes) {
        try {
            return this.fromString(new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException var3) {
            var3.printStackTrace();
            return null;
        }
    }

    public UserInfo deserialize(String s, byte[] bytes) {
        return this.fromBytes(bytes);
    }

    public void configure(Map<String, ?> map, boolean b) {
    }

    public byte[] serialize(String s, UserInfo t) {
        return this.toBytes(t);
    }

    public void close() {
    }
}

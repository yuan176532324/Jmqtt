package com.bigbigcloud.interception;

import java.io.Serializable;

public class DeviceConnMsg implements Serializable {

    private static final long serialVersionUID = -1431584750134928273L;

    private String properties;
    private Integer type;//on/off/unknown 1/0/-1
    private Long ts;
    private String ip;

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}

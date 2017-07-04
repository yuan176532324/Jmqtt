package io.moquette.interception;

import java.io.Serializable;
import java.util.Date;

public class DeviceConnMsg implements Serializable {

    private static final long serialVersionUID = -1431584750134928273L;

    private String deviceGuid;
    private String clientId;
    private long timestamp = (new Date()).getTime();
    private String ip;
    private boolean flag;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public DeviceConnMsg() {
    }

    public String getDeviceGuid() {
        return deviceGuid;
    }

    public void setDeviceGuid(String deviceGuid) {
        this.deviceGuid = deviceGuid;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}

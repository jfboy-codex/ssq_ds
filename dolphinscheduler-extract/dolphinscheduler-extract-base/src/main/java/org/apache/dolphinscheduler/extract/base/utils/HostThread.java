package org.apache.dolphinscheduler.extract.base.utils;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.apache.dolphinscheduler.common.constants.Constants.COLON;

@Data
@Slf4j
public class HostThread {

    Host host;

    Integer threadId;

    public HostThread(Host host, Integer threadId) {
        this.host = host;
        this.threadId = threadId;
    }

    public HostThread(String address) { //192.168.153.1:1234:0
        String[] parts = address.split(COLON);

        this.host = new Host(parts[0], Integer.parseInt(parts[1]));
        if(parts.length == 3) {         //已分配线程
            this.threadId = Integer.parseInt(parts[2]);
        } else if(parts.length == 2) {  //没有分配线程，默认0
            this.threadId = 0;
        } else {
            log.error("分发地址格式错误!!!");
        }
    }

    public static HostThread of(@NonNull String address) {
        return new HostThread(address);
    }
}

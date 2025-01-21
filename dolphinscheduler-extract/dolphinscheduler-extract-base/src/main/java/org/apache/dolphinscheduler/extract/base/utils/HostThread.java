/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.extract.base.utils;

import static org.apache.dolphinscheduler.common.constants.Constants.COLON;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class HostThread {

    Host host;

    Integer threadId;

    public HostThread(Host host, Integer threadId) {
        this.host = host;
        this.threadId = threadId;
    }

    public HostThread(String address) { // 192.168.153.1:1234:0
        String[] parts = address.split(COLON);

        this.host = new Host(parts[0], Integer.parseInt(parts[1]));
        if (parts.length == 3) { // 已分配线程
            this.threadId = Integer.parseInt(parts[2]);
        } else if (parts.length == 2) { // 没有分配线程，默认0
            this.threadId = 0;
        } else {
            log.error("分发地址格式错误!!!");
        }
    }

    public static HostThread of(@NonNull String address) {
        return new HostThread(address);
    }
}

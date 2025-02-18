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

package org.apache.dolphinscheduler.api.service.impl;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.dolphinscheduler.api.service.MonitorService;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.dao.entity.Bandwidth;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.BandwidthMapper;
import org.apache.dolphinscheduler.dao.mapper.HostThreadMapper;
import org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics;
import org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMonitor;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

/**
 * monitor service impl
 */
@Service
@Slf4j
public class MonitorServiceImpl extends BaseServiceImpl implements MonitorService {

    @Autowired
    private DatabaseMonitor databaseMonitor;

    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private BandwidthMapper bandwidthMapper;

    @Autowired
    private HostThreadMapper hostThreadMapper;

    /**
     * query database state
     *
     * @param loginUser login user
     * @return data base state
     */
    @Override
    public List<DatabaseMetrics> queryDatabaseState(User loginUser) {
        return Lists.newArrayList(databaseMonitor.getDatabaseMetrics());
    }

    @Override
    public List<Server> listServer(RegistryNodeType nodeType) {
        return registryClient.getServerList(nodeType);
    }

    @Override
    public boolean setBandwidth(User loginUser, String host) {
        // 清除表中的所有数据
        bandwidthMapper.deleteAllBandwidths();

        List<Bandwidth> bandwidths = new ArrayList<>();
        String[] ports = {"1234", "1244", "1254", "1264"};
        Table<Integer, Integer, Double> bandwidthTable = HashBasedTable.create();
        bandwidthTable.put(0, 0, 100000000.0);
        bandwidthTable.put(0, 1, 60000.0);
        bandwidthTable.put(0, 2, 100000.0);
        bandwidthTable.put(0, 3, 20000.0);
        bandwidthTable.put(1, 0, 80000.0);
        bandwidthTable.put(1, 1, 100000000.0);
        bandwidthTable.put(1, 2, 30000.0);
        bandwidthTable.put(1, 3, 60000.0);
        bandwidthTable.put(2, 0, 80000.0);
        bandwidthTable.put(2, 1, 40000.0);
        bandwidthTable.put(2, 2, 100000000.0);
        bandwidthTable.put(2, 3, 20000.0);
        bandwidthTable.put(3, 0, 40000.0);
        bandwidthTable.put(3, 1, 30000.0);
        bandwidthTable.put(3, 2, 80000.0);
        bandwidthTable.put(3, 3, 100000000.0);

        for (int i=0; i<4; i++) {
            String preHost = host + ":" + ports[i];
            for (int j=0; j<4; j++) {
                String postHost = host + ":" + ports[j];
                bandwidths.add(new Bandwidth(preHost, postHost, bandwidthTable.get(i, j)));
            }
        }
        int rows = bandwidthMapper.insertBandwidths(bandwidths);
        return rows > 0;
    }

    @Override
    public boolean setHostThread(User loginUser, String host) {
        // 清除表中的所有数据
        hostThreadMapper.deleteAllHostThreads();

        List<HostThreadStatus> hostThreads = new ArrayList<>();
        String[] ports = {"1234", "1244", "1254", "1264"};
        Date now = new Date();
        long time = now.getTime();

        for (int i=0; i<4; i++) {
            String hostPort = host + ":" + ports[i];
            for (int j=0; j<4; j++) {
                hostThreads.add(new HostThreadStatus(hostPort, j, now, time));
            }
        }

        int rows = hostThreadMapper.insertHostThreads(hostThreads);
        return rows > 0;
    }
}

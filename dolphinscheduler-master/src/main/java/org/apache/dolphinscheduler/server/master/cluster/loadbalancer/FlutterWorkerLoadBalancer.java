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

package org.apache.dolphinscheduler.server.master.cluster.loadbalancer;


import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.dao.entity.IntermediateData;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.HostThread;
import org.apache.dolphinscheduler.server.master.cluster.IClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;
import org.apache.dolphinscheduler.server.master.cluster.solver.XSolverGurobi;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TasksExecutionRunnable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by random algorithm.
 */
@Slf4j
public class FlutterWorkerLoadBalancer implements IWorkerLoadBalancer {

    private final WorkerClusters workerClusters;

    private final Map<String, WeightedServer<WorkerServerMetadata>> weightedServerMap = new ConcurrentHashMap<>();


    public FlutterWorkerLoadBalancer(WorkerClusters workerClusters) {
        this.workerClusters = workerClusters;

        this.workerClusters.registerListener(new IClusters.IClustersChangeListener<WorkerServerMetadata>() {

            @Override
            public void onServerAdded(WorkerServerMetadata server) {
                weightedServerMap.put(server.getAddress(), new WeightedServer<>(server, hostWeight(server)));
            }

            @Override
            public void onServerRemove(WorkerServerMetadata server) {
                weightedServerMap.remove(server.getAddress());
            }

            @Override
            public void onServerUpdate(WorkerServerMetadata server) {
                weightedServerMap.put(server.getAddress(), new WeightedServer<>(server, hostWeight(server)));
            }

            private double hostWeight(WorkerServerMetadata server) {
                return server.getWorkerWeight();
            }
        });
    }

    @Override
    public Map<Long, HostThread> select(@NotNull String workerGroup, @NotNull TasksExecutionRunnable tasksExecutionRunnable) {

        // 获取服务器集群信息
        List<WeightedServer<WorkerServerMetadata>> weightedServers =
                workerClusters.getNormalWorkerServerAddressByGroup(workerGroup)
                        .stream()
                        .map(weightedServerMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(weightedServers)) {
            return new HashMap<>();
        }

        // 数据中心线程索引
        BiMap<Integer, HostThread> hostThreadBiMap = HashBiMap.create();
        int hostThreadIndex = 0;
        for (WeightedServer<WorkerServerMetadata> weightedServer : weightedServers) { // 遍历j
            Host host = Host.of(weightedServer.getServer().getAddress());
            int threadNum = weightedServer.getServer().getThreadNum();
            for (int threadId = 0; threadId < threadNum; threadId++) {
                hostThreadBiMap.put(hostThreadIndex, new HostThread(host, threadId));
                hostThreadIndex++;
            }
        }

        // 任务索引
        BiMap<Integer, Long> taskBiMap = tasksExecutionRunnable.getTaskBiMap();

        Table<Long, HostThread, Long> timeTable = getXTimeTable(weightedServers, tasksExecutionRunnable); // s(t,j)
        XSolverGurobi xSolverGurobi = new XSolverGurobi(taskBiMap, hostThreadBiMap, timeTable);
        tasksExecutionRunnable.setTasksHostThreadMap(xSolverGurobi.solve());
        long time = xSolverGurobi.getOptimalTime();
        updateThreadId(tasksExecutionRunnable); // 更新线程
        Map<Long, HostThread> taskHostThreadMap = tasksExecutionRunnable.getTasksHostThreadMap();

        System.out.println("*******************************************Flutter算法内部*******************************************");
        System.out.println("---调度参数---");
        System.out.printf("数据中心线程总数: %d\n", hostThreadBiMap.size());
        System.out.printf("任务总数: %d 任务: %s\n", taskBiMap.size(), taskBiMap);
        System.out.println("---调度结果---");
        System.out.println("任务位置: " + taskHostThreadMap);
        System.out.println("最终时间: " + time);
        System.out.println("***************************************************************************************************");

        return taskHostThreadMap;
    }

    public Table<Long, HostThread, Long> getXTimeTable(List<WeightedServer<WorkerServerMetadata>> weightedServers, TasksExecutionRunnable tasksExecutionRunnable) {

        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        Table<Host, Host, Double> bandwidthTable = tasksExecutionRunnable.getBandwidthTable();
        Table<Long, Long, IntermediateData> tasksNeedTasksDataTable = tasksExecutionRunnable.getTasksNeedTasksData();
        Map<Long, HostThread> dataHostThreadMap = tasksExecutionRunnable.getDataHostThreadMap();

        Table<Long, HostThread, Long> timeTable = HashBasedTable.create(); // s(t,j)
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            long exeTime = DateUtils.duration2MS(taskInstance.getExeTime());
            long taskCode = taskInstance.getTaskCode();
            for (WeightedServer<WorkerServerMetadata> weightedServer : weightedServers) { // 遍历j
                Host taskHost = Host.of(weightedServer.getServer().getAddress());
                int threadNum = weightedServer.getServer().getThreadNum();
                for (int threadId = 0; threadId < threadNum; threadId++) {

                    HostThread hostThread = new HostThread(taskHost, threadId);

                    // 计算传输时间，由所需各数据的最大传输时间决定
                    long transferTime = 0;
                    Map<Long, IntermediateData> dataMap = tasksNeedTasksDataTable.rowMap().get(taskCode);
                    if(dataMap!=null) {
                        for (Map.Entry<Long, IntermediateData> entry : dataMap.entrySet()) {
                            // tasksNeedTasksData的host可能不准确了
                            Host dataHost = dataHostThreadMap.get(entry.getKey()).getHost();
                            long dataSize = entry.getValue().getDataSize();
                            Double band = bandwidthTable.get(dataHost, taskHost);
                            double bandwidth = 1;
                            if (band != null) bandwidth = band;
                            long localTransferTime = (long) (dataSize * 1000 / bandwidth); //单位ms
                            if (localTransferTime >= transferTime) {
                                transferTime = localTransferTime;
                            }
                        }
                    }

                    // 计算完成时间
                    long completionTime = transferTime + exeTime;
                    timeTable.put(taskCode, hostThread, completionTime);
                }
            }
        }
        return timeTable;
    }

    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.FLUTTER;
    }

    @Override
    public boolean isMultiDispatcher() { return true; }
}

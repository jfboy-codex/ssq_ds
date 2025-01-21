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

import org.apache.dolphinscheduler.common.utils.DateUtils;
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

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by random algorithm.
 */
@Slf4j
public class MaxMinFairnessWorkerLoadBalancer implements IWorkerLoadBalancer {

    private final WorkerClusters workerClusters;

    private final Map<String, WeightedServer<WorkerServerMetadata>> weightedServerMap = new ConcurrentHashMap<>();

    public MaxMinFairnessWorkerLoadBalancer(WorkerClusters workerClusters) {
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
    public Map<Long, HostThread> select(@NotNull String workerGroup,
                                        @NotNull TasksExecutionRunnable tasksExecutionRunnable) {

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
        BiMap<Integer, HostThread> dynamicHostThreadBiMap = HashBiMap.create(hostThreadBiMap);

        // 任务索引
        BiMap<Integer, Long> taskBiMap = tasksExecutionRunnable.getTaskBiMap();
        BiMap<Integer, Long> dynamicTaskBiMap = HashBiMap.create(taskBiMap);

        Table<Long, HostThread, Long> timeTable = getXTimeTable(weightedServers, tasksExecutionRunnable); // s(t,j)
        Map<Long, HostThread> optimalTaskHostThreadMap = new HashMap<>();
        List<Long> timeList = new ArrayList<>();

        int num = taskBiMap.size();
        while (num > 0) {
            XSolverGurobi xSolverGurobi = new XSolverGurobi(dynamicTaskBiMap, dynamicHostThreadBiMap, timeTable);
            Map<Long, HostThread> taskHostThreadMap = xSolverGurobi.solve();
            long optimalTime = xSolverGurobi.getOptimalTime();
            timeList.add(optimalTime);

            // 找到最长任务从x参数列表中去除，更新线程列表
            for (Map.Entry<Long, HostThread> taskHostThread : taskHostThreadMap.entrySet()) {
                long taskCode = taskHostThread.getKey();
                HostThread hostThread = taskHostThread.getValue();
                if (timeTable.get(taskCode, hostThread) == optimalTime) {
                    optimalTaskHostThreadMap.put(taskCode, hostThread);
                    dynamicTaskBiMap = getNewTaskBiMap(dynamicTaskBiMap, taskCode);
                    dynamicHostThreadBiMap = getNewHostThreadBiMap(dynamicHostThreadBiMap, hostThread);
                    break;
                }
            }
            num--;
        }

        tasksExecutionRunnable.setTasksHostThreadMap(optimalTaskHostThreadMap);
        updateThreadId(tasksExecutionRunnable); // 更新线程
        Map<Long, HostThread> taskHostThreadMap = tasksExecutionRunnable.getTasksHostThreadMap();

        System.out.println(
                "***************************************MaxMinFairness算法内部***************************************");
        System.out.println("---调度参数---");
        System.out.printf("数据中心线程总数: %d\n", hostThreadBiMap.size());
        System.out.printf("任务总数: %d 任务: %s\n", taskBiMap.size(), taskBiMap);
        System.out.println("---调度过程---");
        System.out.printf("迭代次数: %d 迭代结果: %s\n", timeList.size(), timeList);
        System.out.println("---调度结果---");
        System.out.println("任务位置: " + taskHostThreadMap);
        System.out.println(
                "***************************************************************************************************");

        return taskHostThreadMap;
    }

    public BiMap<Integer, Long> getNewTaskBiMap(BiMap<Integer, Long> taskBiMap, long deleteTaskCode) {
        BiMap<Integer, Long> newTaskBiMap = HashBiMap.create();
        int index = 0;
        for (int i = 0; i < taskBiMap.size(); i++) {
            long taskCode = taskBiMap.get(i);
            if (taskCode != deleteTaskCode) {
                newTaskBiMap.put(index, taskCode);
                index++;
            }
        }
        return newTaskBiMap;
    }

    public BiMap<Integer, HostThread> getNewHostThreadBiMap(BiMap<Integer, HostThread> hostThreadBiMap,
                                                            HostThread deleteHostThread) {
        BiMap<Integer, HostThread> newHostThreadBiMap = HashBiMap.create();
        int index = 0;
        for (int i = 0; i < hostThreadBiMap.size(); i++) {
            HostThread hostThread = hostThreadBiMap.get(i);
            if (hostThread.getHost() != deleteHostThread.getHost()
                    || !Objects.equals(hostThread.getThreadId(), deleteHostThread.getThreadId())) {
                newHostThreadBiMap.put(index, hostThread);
                index++;
            }
        }
        return newHostThreadBiMap;
    }

    public Table<Long, HostThread, Long> getXTimeTable(List<WeightedServer<WorkerServerMetadata>> weightedServers,
                                                       TasksExecutionRunnable tasksExecutionRunnable) {

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
                    if (dataMap != null) {
                        for (Map.Entry<Long, IntermediateData> entry : dataMap.entrySet()) {
                            // tasksNeedTasksData的host可能不准确了
                            Host dataHost = dataHostThreadMap.get(entry.getKey()).getHost();
                            long dataSize = entry.getValue().getDataSize();
                            Double band = bandwidthTable.get(dataHost, taskHost);
                            double bandwidth = 1;
                            if (band != null)
                                bandwidth = band;
                            long localTransferTime = (long) (dataSize * 1000 / bandwidth); // 单位ms
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
        return WorkerLoadBalancerType.MAX_MIN_FAIRNESS;
    }

    @Override
    public boolean isMultiDispatcher() {
        return true;
    }
}

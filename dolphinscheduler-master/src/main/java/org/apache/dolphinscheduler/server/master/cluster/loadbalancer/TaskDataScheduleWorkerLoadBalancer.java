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
import org.apache.dolphinscheduler.server.master.cluster.solver.YSolverGurobi;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TasksExecutionRunnable;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;

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
public class TaskDataScheduleWorkerLoadBalancer implements IWorkerLoadBalancer {

    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    private final WorkerClusters workerClusters;

    private final Map<String, WeightedServer<WorkerServerMetadata>> weightedServerMap = new ConcurrentHashMap<>();

    public TaskDataScheduleWorkerLoadBalancer(WorkerClusters workerClusters) {
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
            int threadNum = weightedServer.getServer().getThreadNum();
            for (int threadId = 0; threadId < threadNum; threadId++) {
                Host postHost = Host.of(weightedServer.getServer().getAddress());
                hostThreadBiMap.put(hostThreadIndex, new HostThread(postHost, threadId));
                hostThreadIndex++;
            }
        }

        // 任务索引
        BiMap<Integer, Long> taskBiMap = tasksExecutionRunnable.getTaskBiMap();

        // 数据索引
        BiMap<Integer, Long> dataBiMap = tasksExecutionRunnable.getDataBiMap();

        List<Long> timeList = new ArrayList<>();

        if (dataBiMap.isEmpty()) { // 没有数据就只进行任务调度
            Table<Long, HostThread, Long> timeTable = getXTimeTable(weightedServers, tasksExecutionRunnable); // s(t,j)
            XSolverGurobi xSolverGurobi = new XSolverGurobi(taskBiMap, hostThreadBiMap, timeTable);
            tasksExecutionRunnable.setTasksHostThreadMap(xSolverGurobi.solve());
            timeList.add(xSolverGurobi.getOptimalTime());
        } else { // 任务调度和数据调度迭代
            int num = 0;
            while (num < 10) {
                if (num % 2 == 0) {
                    Table<Long, HostThread, Long> timeTable = getXTimeTable(weightedServers, tasksExecutionRunnable); // s(t,j)
                    XSolverGurobi xSolverGurobi = new XSolverGurobi(taskBiMap, hostThreadBiMap, timeTable);
                    tasksExecutionRunnable.setTasksHostThreadMap(xSolverGurobi.solve());
                    timeList.add(xSolverGurobi.getOptimalTime());
                    if (timeList.size() > 1) {
                        if (Objects.equals(timeList.get(timeList.size() - 1), timeList.get(timeList.size() - 2)))
                            break;
                    }
                } else {
                    Table<Long, HostThread, Long> timeTable = getYTimeTable(weightedServers, tasksExecutionRunnable);
                    YSolverGurobi ySolverGurobi = new YSolverGurobi(dataBiMap, hostThreadBiMap, timeTable);
                    tasksExecutionRunnable.setDataHostThreadMap(ySolverGurobi.solve());
                    timeList.add(ySolverGurobi.getOptimalTime());
                    if (Objects.equals(timeList.get(timeList.size() - 1), timeList.get(timeList.size() - 2)))
                        break;
                }
                num++;
            }
        }

        Map<Long, HostThread> taskHostThreadMap = tasksExecutionRunnable.getTasksHostThreadMap();
        Map<Long, HostThread> dataHostThreadMap = tasksExecutionRunnable.getDataHostThreadMap();

        System.out.println(
                "****************************************任务数据协同调度算法内部****************************************");
        System.out.println("---调度参数---");
        System.out.printf("数据中心线程总数: %d\n", hostThreadBiMap.size());
        System.out.printf("任务总数: %d 任务: %s\n", taskBiMap.size(), taskBiMap);
        System.out.printf("数据总数: %d 数据: %s\n", dataBiMap.size(), dataBiMap);
        System.out.println("---调度过程---");
        System.out.printf("迭代次数: %d 迭代结果: %s\n", timeList.size(), timeList);
        System.out.println("---调度结果---");
        System.out.println("任务位置: " + taskHostThreadMap);
        System.out.println("数据位置: " + dataHostThreadMap);
        System.out.println(
                "***************************************************************************************************");

        // 更新数据库数据位置
        Table<Long, Long, IntermediateData> tasksNeedTasksData = tasksExecutionRunnable.getTasksNeedTasksData();
        for (Table.Cell<Long, Long, IntermediateData> cell : tasksNeedTasksData.cellSet()) {
            TaskInstance taskInstance = cell.getValue().getTaskInstance();
            long taskCode = taskInstance.getTaskCode();
            HostThread dataHostThread = dataHostThreadMap.get(taskCode);
            if (dataHostThread != null) {
                // TODO tasksNeedTasksData更新
                taskInstance.setDataHost(dataHostThread.getHost().getAddress());
            }
            processService.updateTaskInstance(taskInstance);
        }
        return taskHostThreadMap;
    }

    public Table<Long, HostThread, Long> getYTimeTable(List<WeightedServer<WorkerServerMetadata>> weightedServers,
                                                       TasksExecutionRunnable tasksExecutionRunnable) {

        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        Table<Host, Host, Double> bandwidthTable = tasksExecutionRunnable.getBandwidthTable();
        Table<Long, Long, IntermediateData> tasksNeedTasksDataTable = tasksExecutionRunnable.getTasksNeedTasksData();
        Table<Host, Integer, Long> hostUsableTimeTable = tasksExecutionRunnable.getHostUsableTimeTable();
        BiMap<Integer, Long> dataBiMap = tasksExecutionRunnable.getDataBiMap();
        Map<Long, HostThread> tasksHostThreadMap = tasksExecutionRunnable.getTasksHostThreadMap();

        Table<Long, HostThread, Long> timeTable = HashBasedTable.create(); // s(t,j)
        for (Map.Entry<Integer, Long> data : dataBiMap.entrySet()) {
            long dataCode = data.getValue();
            for (WeightedServer<WorkerServerMetadata> weightedServer : weightedServers) { // 遍历j
                Host dataHost = Host.of(weightedServer.getServer().getAddress());
                int threadNum = weightedServer.getServer().getThreadNum();
                for (int threadId = 0; threadId < threadNum; threadId++) {
                    HostThread hostThread = new HostThread(dataHost, threadId);
                    long time = 0;
                    for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
                        TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
                        long taskCode = taskInstance.getTaskCode();
                        HostThread taskHostThread = tasksHostThreadMap.get(taskCode);

                        // 计算等待时间
                        long waitTime = 0;
                        Long usableTime =
                                hostUsableTimeTable.get(taskHostThread.getHost(), taskHostThread.getThreadId());
                        if (usableTime != null && usableTime > taskInstance.getSubmitTime().getTime()) {
                            waitTime = usableTime - taskInstance.getSubmitTime().getTime();
                        }

                        // 计算传输时间
                        long dataSize = 0;
                        IntermediateData iData = tasksNeedTasksDataTable.get(taskCode, dataCode);
                        if (iData != null)
                            dataSize = iData.getDataSize();
                        Double band = bandwidthTable.get(dataHost, taskHostThread.getHost());
                        double bandwidth = 1;
                        if (band != null)
                            bandwidth = band;
                        long transferTime = (long) (dataSize * 1000 / bandwidth);

                        // 计算执行时间
                        long exeTime = DateUtils.duration2MS(taskInstance.getExeTime());

                        // 计算完成时间
                        long completionTime = Math.max(waitTime, transferTime) + exeTime;

                        if (completionTime > time) {
                            time = completionTime;
                        }
                    }
                    timeTable.put(dataCode, hostThread, time);
                }
            }
        }
        return timeTable;
    }

    public Table<Long, HostThread, Long> getXTimeTable(List<WeightedServer<WorkerServerMetadata>> weightedServers,
                                                       TasksExecutionRunnable tasksExecutionRunnable) {

        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        Table<Host, Host, Double> bandwidthTable = tasksExecutionRunnable.getBandwidthTable();
        Table<Long, Long, IntermediateData> tasksNeedTasksDataTable = tasksExecutionRunnable.getTasksNeedTasksData();
        Table<Host, Integer, Long> hostUsableTimeTable = tasksExecutionRunnable.getHostUsableTimeTable();
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

                    // 计算等待时间
                    long waitTime = 0;
                    Long usableTime = hostUsableTimeTable.get(taskHost, threadId);
                    if (usableTime != null && usableTime > taskInstance.getSubmitTime().getTime()) {
                        waitTime = usableTime - taskInstance.getSubmitTime().getTime();
                    }

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
                    // for (Table.Cell<Long, Long, IntermediateData> cell : tasksNeedTasksDataTable.cellSet()) {
                    // // Host dataHost = Host.of(cell.getValue().getHost()); // 初始位置
                    // Host dataHost = dataHostThreadMap.get(cell.getValue().getTaskInstance().getTaskCode()).getHost();
                    // Double band = bandwidthTable.get(dataHost, taskHost);
                    // double bandwidth = 1;
                    // if (band != null) bandwidth = band;
                    // long localTransferTime = (long) (cell.getValue().getDataSize() * 1000 / bandwidth);
                    // if (localTransferTime > transferTime) transferTime = localTransferTime;
                    // }

                    // 计算完成时间
                    long completionTime = Math.max(waitTime, transferTime) + exeTime;
                    timeTable.put(taskCode, hostThread, completionTime);
                }
            }
        }
        return timeTable;
    }

    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.TASK_DATA_SCHEDULER;
    }

    @Override
    public boolean isMultiDispatcher() {
        return true;
    }
}

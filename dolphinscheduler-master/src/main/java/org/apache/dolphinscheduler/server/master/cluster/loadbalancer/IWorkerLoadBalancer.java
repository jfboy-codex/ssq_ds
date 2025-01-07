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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.HostThread;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;

import java.util.*;
import java.util.stream.Collectors;

import lombok.NonNull;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TasksExecutionRunnable;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by load balancer algorithm.
 */
public interface IWorkerLoadBalancer {

    /**
     * Select a worker address under the given worker group.
     *
     * @param workerGroup worker group cannot be null.
     * @return the selected worker address, or empty if no worker is available.
     */
    default Optional<String> select(@NonNull String workerGroup) {
        return Optional.empty();
    }

    default Optional<String> select(@NonNull String workerGroup, @NonNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        return Optional.empty();
    }

    default Map<Long, HostThread> select(@NonNull String workerGroup, @NonNull TasksExecutionRunnable tasksExecutionRunnable) {
        return new HashMap<>();
    }

    default int getFirstUsableThreadId(@NonNull String host, @NonNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        List<HostThreadStatus> hostThreadStatusList = ITaskExecutionRunnable.getHostThreadStatusList();
        List<HostThreadStatus> threadStatusList = hostThreadStatusList.stream()
                .filter(v -> host.equals(v.getHost()))
                .collect(Collectors.toList());
        threadStatusList.sort((p1,p2) -> (int) (p1.getUsableTimeStamp() - p2.getUsableTimeStamp())); // 排序
        return threadStatusList.get(0).getThreadId();
    }

    default void updateThreadId(@NonNull TasksExecutionRunnable tasksExecutionRunnable){
        Map<Long, HostThread> taskHostThreadMap = tasksExecutionRunnable.getTasksHostThreadMap();
        List<HostThreadStatus> hostThreadStatusList = tasksExecutionRunnable.getHostThreadStatusList();
        hostThreadStatusList.sort((p1,p2) -> (int) (p1.getUsableTimeStamp() - p2.getUsableTimeStamp()));
        Map<Long, HostThread> newMap = new HashMap<>();
        List<HostThreadStatus> sortList = new ArrayList<>(hostThreadStatusList);
        for(Map.Entry<Long, HostThread> entry : taskHostThreadMap.entrySet()) {
            for(HostThreadStatus hostThreadStatus : sortList) {
                HostThread oldHostThread = entry.getValue();
                Host host = oldHostThread.getHost();
                if(Objects.equals(hostThreadStatus.getHost(), host.getAddress())) {
                    newMap.put(entry.getKey(), new HostThread(host, hostThreadStatus.getThreadId()));
                    sortList.remove(hostThreadStatus);
                    break;
                }
            }
        }
        tasksExecutionRunnable.setTasksHostThreadMap(newMap);
    }

    WorkerLoadBalancerType getType();

    boolean isMultiDispatcher(); // 是否支持多任务调度

    default String hostThreadToAddress(HostThread hostThread) {
        Host host = hostThread.getHost();
        return host.getIp() + ":" + host.getPort() + ":" + hostThread.getThreadId();
    }
}

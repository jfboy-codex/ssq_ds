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

import lombok.Data;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;

import org.apache.commons.collections4.CollectionUtils;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.jetbrains.annotations.NotNull;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by random algorithm.
 */
@Data
public class RandomWorkerLoadBalancer implements IWorkerLoadBalancer {

    private final WorkerClusters workerClusters;

    private final SecureRandom secureRandom;

    public RandomWorkerLoadBalancer(WorkerClusters workerClusters) {
        this.workerClusters = workerClusters;
        this.secureRandom = new SecureRandom();
    }

    @Override
    public Optional<String> select(@NotNull String workerGroup, @NotNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        List<String> workerServerAddresses = workerClusters.getNormalWorkerServerAddressByGroup(workerGroup); // 获取状态正常的服务器地址列表
        if (CollectionUtils.isEmpty(workerServerAddresses)) {
            return Optional.empty();
        }
        int index = secureRandom.nextInt(workerServerAddresses.size()); // 生成[0,size)的一个随机数
        String host = workerServerAddresses.get(index);
        int threadId = getFirstUsableThreadId(host, ITaskExecutionRunnable);

        return Optional.of(host + ":" + threadId);
    }

    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.RANDOM;
    }

    @Override
    public boolean isMultiDispatcher() { return false; }
}

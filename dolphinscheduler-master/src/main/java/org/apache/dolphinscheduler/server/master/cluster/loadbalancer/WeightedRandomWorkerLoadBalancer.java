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

import org.apache.dolphinscheduler.server.master.cluster.IClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;
import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import org.apache.commons.collections4.CollectionUtils;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.jetbrains.annotations.NotNull;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by random algorithm.
 */
@Data
@Slf4j
public class WeightedRandomWorkerLoadBalancer implements IWorkerLoadBalancer {

    private final WorkerClusters workerClusters;

    private final SecureRandom secureRandom;

    private Map<String, WeightedServer<WorkerServerMetadata>> weightedServerMap = new ConcurrentHashMap<>();

    public WeightedRandomWorkerLoadBalancer(WorkerClusters workerClusters) {
        this.workerClusters = workerClusters;
        this.secureRandom = new SecureRandom();

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
    public Optional<String> select(@NotNull String workerGroup,
                                   @NotNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        List<WeightedServer<WorkerServerMetadata>> weightedServers =
                workerClusters.getNormalWorkerServerAddressByGroup(workerGroup)
                        .stream()
                        .map(weightedServerMap::get)
                        .filter(Objects::nonNull) // filter non null here to avoid the two map changed between
                        // workerClusters
                        // and weightedServerMap is not atomic
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(weightedServers)) {
            return Optional.empty();
        }

        double totalWeight = weightedServers.stream().mapToDouble(WeightedServer::getWeight).sum();

        int intTotalWeight = (int) Math.round(totalWeight);

        int R = secureRandom.nextInt(intTotalWeight);

        double sumWeight = 0;
        WeightedServer<WorkerServerMetadata> selectedWorker = null;
        for (WeightedServer<WorkerServerMetadata> tmpWorker : weightedServers) {
            sumWeight += tmpWorker.getWeight();
            if (sumWeight >= R) {
                selectedWorker = tmpWorker;
                break;
            }
        }
        String host = selectedWorker.getServer().getAddress();
        int threadId = getFirstUsableThreadId(host, ITaskExecutionRunnable);

        return Optional.of(host + ":" + threadId);
    }

    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.WEIGHTED_RANDOM;
    }

    @Override
    public boolean isMultiDispatcher() {
        return false;
    }
}

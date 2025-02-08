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

import org.apache.dolphinscheduler.extract.base.utils.HostThread;
import org.apache.dolphinscheduler.server.master.cluster.WorkerClusters;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TasksExecutionRunnable;

import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;

/**
 * The worker load balancer used to select a worker from the {@link WorkerClusters} by random algorithm.
 */
public class FreedomWorkerLoadBalancer implements IWorkerLoadBalancer {

    private RandomWorkerLoadBalancer randomWorkerLoadBalancer;

    private RoundRobinWorkerLoadBalancer roundRobinWorkerLoadBalancer;

    private FixedWeightedRoundRobinWorkerLoadBalancer fixedWeightedRoundRobinWorkerLoadBalancer;

    private DynamicWeightedRoundRobinWorkerLoadBalancer dynamicWeightedRoundRobinWorkerLoadBalancer;

    private WeightedRandomWorkerLoadBalancer weightedRandomWorkerLoadBalancer;

    private MaxMinFairnessWorkerLoadBalancer maxMinFairnessWorkerLoadBalancer;

    private FlutterWorkerLoadBalancer flutterWorkerLoadBalancer;

    private HungaryWorkerLoadBalancer hungaryWorkerLoadBalancer;

    private TaskDataScheduleWorkerLoadBalancer taskDataScheduleWorkerLoadBalancer;

    private OnLineTaskScheduleWorkerLoadBalancer onLineTaskScheduleWorkerLoadBalancer;

    private BinaryHungaryDfsWorkerLoadBalancer binaryHungaryDfsWorkerLoadBalancer;

    private BinaryHungaryBfsWorkerLoadBalancer binaryHungaryBfsWorkerLoadBalancer;

    private BinaryHungaryGreedyWorkerLoadBalancer binaryHungaryGreedyWorkerLoadBalancer;

    public FreedomWorkerLoadBalancer(WorkerClusters workerClusters) {
        this.getAllWorkerLoadBalancer(workerClusters);
    }

    public void getAllWorkerLoadBalancer(WorkerClusters workerClusters) {
        randomWorkerLoadBalancer = new RandomWorkerLoadBalancer(workerClusters);
        roundRobinWorkerLoadBalancer = new RoundRobinWorkerLoadBalancer(workerClusters);
        fixedWeightedRoundRobinWorkerLoadBalancer = new FixedWeightedRoundRobinWorkerLoadBalancer(workerClusters);
        dynamicWeightedRoundRobinWorkerLoadBalancer = new DynamicWeightedRoundRobinWorkerLoadBalancer(workerClusters,
                new WorkerLoadBalancerConfigurationProperties().getDynamicWeightConfigProperties());
        weightedRandomWorkerLoadBalancer = new WeightedRandomWorkerLoadBalancer(workerClusters);
        maxMinFairnessWorkerLoadBalancer = new MaxMinFairnessWorkerLoadBalancer(workerClusters);
        flutterWorkerLoadBalancer = new FlutterWorkerLoadBalancer(workerClusters);
        hungaryWorkerLoadBalancer = new HungaryWorkerLoadBalancer(workerClusters);
        taskDataScheduleWorkerLoadBalancer = new TaskDataScheduleWorkerLoadBalancer(workerClusters);
        onLineTaskScheduleWorkerLoadBalancer = new OnLineTaskScheduleWorkerLoadBalancer(workerClusters);
        binaryHungaryDfsWorkerLoadBalancer = new BinaryHungaryDfsWorkerLoadBalancer(workerClusters);
        binaryHungaryBfsWorkerLoadBalancer = new BinaryHungaryBfsWorkerLoadBalancer(workerClusters);
        binaryHungaryGreedyWorkerLoadBalancer = new BinaryHungaryGreedyWorkerLoadBalancer(workerClusters);
    }

    @Override
    public Map<Long, HostThread> select(@NotNull String workerGroup,
                                        @NotNull TasksExecutionRunnable tasksExecutionRunnable) {
        String type = tasksExecutionRunnable.getDispatchType();
        switch (type) {
            case "MAX_MIN_FAIRNESS":
                return maxMinFairnessWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "FLUTTER":
                return flutterWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "HUNGARY":
                return hungaryWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "TASK_DATA_SCHEDULE":
                return taskDataScheduleWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "ON_LINE_TASK_SCHEDULE":
                return onLineTaskScheduleWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "BINARY_HUNGARY_DFS":
                return binaryHungaryDfsWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "BINARY_HUNGARY_BFS":
                return binaryHungaryBfsWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            case "BINARY_HUNGARY_GREEDY":
                return binaryHungaryGreedyWorkerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            default:
                throw new IllegalArgumentException("不支持调度策略 " + type);
        }
    }

    @Override
    public Optional<String> select(@NotNull String workerGroup,
                                   @NotNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        String type = ITaskExecutionRunnable.getWorkflowInstance().getDispatchType();
        switch (type) {
            case "RANDOM":
                return randomWorkerLoadBalancer.select(workerGroup, ITaskExecutionRunnable);
            case "ROUND_ROBIN":
                return roundRobinWorkerLoadBalancer.select(workerGroup, ITaskExecutionRunnable);
            case "FIXED_WEIGHTED_ROUND_ROBIN":
                return fixedWeightedRoundRobinWorkerLoadBalancer.select(workerGroup, ITaskExecutionRunnable);
            case "DYNAMIC_WEIGHTED_ROUND_ROBIN":
                return dynamicWeightedRoundRobinWorkerLoadBalancer.select(workerGroup, ITaskExecutionRunnable);
            case "WEIGHTED_RANDOM":
                return weightedRandomWorkerLoadBalancer.select(workerGroup, ITaskExecutionRunnable);
            default:
                throw new IllegalArgumentException("不支持调度策略 " + type);
        }
    }

    @Override
    public WorkerLoadBalancerType getType() {
        return WorkerLoadBalancerType.FREEDOM;
    }

    @Override
    public boolean isMultiDispatcher() {
        return true;
    }
}

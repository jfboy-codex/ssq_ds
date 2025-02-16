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

package org.apache.dolphinscheduler.server.master.runner.dispatcher;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.Bandwidth;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.dao.entity.IntermediateData;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.extract.base.client.Clients;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.HostThread;
import org.apache.dolphinscheduler.extract.worker.ITaskInstanceOperator;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceDispatchRequest;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceDispatchResponse;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.cluster.ClusterManager;
import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;
import org.apache.dolphinscheduler.server.master.cluster.loadbalancer.IWorkerLoadBalancer;
import org.apache.dolphinscheduler.server.master.cluster.loadbalancer.WorkerLoadBalancerType;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TasksExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

@Slf4j
@Component
public class WorkerTaskDispatcher extends BaseTaskDispatcher {

    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    private final ClusterManager clusterManager = SpringApplicationContext.getBean(ClusterManager.class);

    private final IWorkerLoadBalancer workerLoadBalancer;

    public WorkerTaskDispatcher(IWorkerLoadBalancer workerLoadBalancer) {
        this.workerLoadBalancer = checkNotNull(workerLoadBalancer);
    }

    @Override
    protected void doDispatch(ITaskExecutionRunnable ITaskExecutionRunnable) throws TaskDispatchException {
        final TaskExecutionContext taskExecutionContext = ITaskExecutionRunnable.getTaskExecutionContext();
        final String taskName = taskExecutionContext.getTaskName();
        final String workerAddress = taskExecutionContext.getHost();
        try {
            final TaskInstanceDispatchResponse taskInstanceDispatchResponse = Clients
                    .withService(ITaskInstanceOperator.class)
                    .withHost(workerAddress)
                    .dispatchTask(new TaskInstanceDispatchRequest(ITaskExecutionRunnable.getTaskExecutionContext()));
            if (!taskInstanceDispatchResponse.isDispatchSuccess()) {
                throw new TaskDispatchException("Dispatch task: " + taskName + " to " + workerAddress + " failed: "
                        + taskInstanceDispatchResponse);
            }
        } catch (TaskDispatchException e) {
            throw e;
        } catch (Exception e) {
            throw new TaskDispatchException("Dispatch task: " + taskName + " to " + workerAddress + " failed", e);
        }
    }

    @Override
    protected Map<Long, Optional<Host>> getTaskInstancesDispatchHosts(List<ITaskExecutionRunnable> taskExecutionRunnableList) {
        // 获取并设置算法需要的信息，如数据中心带宽，线程状态等等
        TasksExecutionRunnable tasksExecutionRunnable = setInformation(taskExecutionRunnableList);
        // 获取任务组
        String workerGroup = tasksExecutionRunnable.getWorkerGroup();

        Map<Long, Optional<Host>> tasksDispatchHosts = new HashMap<>();

        // 如果配置的算法不支持多任务调度就循环执行单任务调度
        if (!workerLoadBalancer.isMultiDispatcher()) {
            for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
                TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
                tasksDispatchHosts.put(taskInstance.getTaskCode(), getTaskInstanceDispatchHost(taskExecutionRunnable));
            }
        } else {
            // 使用多任务调度算法分发多个任务
            Map<Long, HostThread> tasksDispatchHostThread =
                    workerLoadBalancer.select(workerGroup, tasksExecutionRunnable);
            for (Map.Entry<Long, HostThread> entry : tasksDispatchHostThread.entrySet()) {
                tasksDispatchHosts.put(entry.getKey(), Optional.of(entry.getValue().getHost()));
            }
            long dispatchTime = new Date().getTime() - tasksExecutionRunnable.getDispatchCode();
            setTasksInstanceInfo(tasksExecutionRunnable, tasksDispatchHostThread, dispatchTime);
            setHostsThreadUsableTime(tasksExecutionRunnable, tasksDispatchHostThread);
        }
        return tasksDispatchHosts;
    }

    // synchronized可以不用要，GlobalTaskDispatchWaitingQueueLooper已经确保分发不会同时进行
    // 如果多线程分发的话需要带上synchronized避免hostThread表的脏读
    @Override
    protected synchronized Optional<Host> getTaskInstanceDispatchHost(ITaskExecutionRunnable ITaskExecutionRunnable) {

        String workerGroup = ITaskExecutionRunnable.getTaskExecutionContext().getWorkerGroup();
        WorkerLoadBalancerType configType = workerLoadBalancer.getType();
        String dispatchType = ITaskExecutionRunnable.getWorkflowInstance().getDispatchType();
        int dataRate = ITaskExecutionRunnable.getWorkflowInstance().getDataRate();
        TaskInstance taskInstance = ITaskExecutionRunnable.getTaskInstance();

        if ((configType == WorkerLoadBalancerType.FREEDOM && Objects.equals(dispatchType, "ON_LINE_TASK_SCHEDULE"))
                || configType == WorkerLoadBalancerType.ON_LINE_TASK_SCHEDULER) { // 在线任务调度
            List<ITaskExecutionRunnable> taskExecutionRunnableList = new ArrayList<>();
            taskExecutionRunnableList.add(ITaskExecutionRunnable);
            Map<Long, Optional<Host>> taskHostMap = getTaskInstancesDispatchHosts(taskExecutionRunnableList);
            long taskCode = ITaskExecutionRunnable.getTaskInstance().getTaskCode();
            return taskHostMap.get(taskCode);
        } else {
            // 获取信息
            long dispatchStartTime = new Date().getTime();
            ITaskExecutionRunnable.setBandwidthList(processService.findDataCenterBandwidth()); // 获取节点带宽信息
            ITaskExecutionRunnable.setTaskNeedHostData(getHostData(ITaskExecutionRunnable, dataRate)); // 获取任务数据信息
            long exeTime = predictExeTime(ITaskExecutionRunnable);
            taskInstance.setDispatchCode(dispatchStartTime);
            taskInstance.setExeTime(DateUtils.format2DurationMS(exeTime)); // 预测任务执行时间（选取前要用）
            // 获取线程负载信息
            List<HostThreadStatus> allHostThreadStatusList = processService.queryHostThreadStatus();
            Map<Host, Integer> hostThreadNum = getHostThreadNum();
            List<HostThreadStatus> hostThreadStatusList = new ArrayList<>();
            for (HostThreadStatus hostThreadStatus : allHostThreadStatusList) {
                int threadNum = 0;
                if (hostThreadNum.get(Host.of(hostThreadStatus.getHost())) != null) {
                    threadNum = hostThreadNum.get(Host.of(hostThreadStatus.getHost()));
                    if (hostThreadStatus.getThreadId() < threadNum) {
                        hostThreadStatusList.add(hostThreadStatus);
                    }
                }
            }
            ITaskExecutionRunnable.setHostThreadStatusList(hostThreadStatusList);

            // 分发
            Optional<HostThread> optionalHostThread =
                    workerLoadBalancer.select(workerGroup, ITaskExecutionRunnable).map(HostThread::of);
            HostThread hostThread = optionalHostThread.get();

            // 更新任务实例相关字段信息
            taskInstance.setThreadId(optionalHostThread.get().getThreadId()); // 设置线程ID
            long waitTime = setWaitTime(ITaskExecutionRunnable, optionalHostThread); // 设置等待时间
            long transferTime = setTaskTransferTime(ITaskExecutionRunnable, optionalHostThread); // 设置传输时间
            long completionTime = Math.max(waitTime, transferTime) + exeTime;
            taskInstance.setCompletionTime(DateUtils.format2DurationMS(completionTime)); // 设置完成时间
            float ru = 100;
            if (transferTime > waitTime) {
                long tw = transferTime - waitTime;
                if (exeTime != 0) {
                    ru = (float) exeTime / (exeTime + tw) * 100;
                }
            }
            String resourceUtilization = String.format("%.2f", ru) + "%";
            taskInstance.setResourceUtilization(resourceUtilization); // 设置资源利用率
            long dispatchEndTime = new Date().getTime();
            taskInstance.setDispatchTime(DateUtils.format2DurationMS(dispatchEndTime - dispatchStartTime));

            // 更新host线程可用时间
            setHostThreadUsableTime(ITaskExecutionRunnable, hostThread, transferTime, exeTime);

            return Optional.of(hostThread.getHost());
        }

    }

    public Map<Host, Integer> getHostThreadNum() {
        Map<String, WorkerServerMetadata> workerMapping = this.clusterManager.getWorkerClusters().getWorkerMapping();
        Map<Host, Integer> hostThreadNum = new HashMap<>();
        for (Map.Entry<String, WorkerServerMetadata> entry : workerMapping.entrySet()) {
            hostThreadNum.put(Host.of(entry.getKey()), entry.getValue().getThreadNum());
        }
        return hostThreadNum;
    }

    public TasksExecutionRunnable setInformation(@NonNull List<ITaskExecutionRunnable> taskExecutionRunnableList) {
        TasksExecutionRunnable tasksExecutionRunnable = new TasksExecutionRunnable();
        tasksExecutionRunnable.setDispatchCode(new Date().getTime()); // 调度编号

        // TODO 任务组里面的分发策略以及工作组应该保持一致，这里暂选一个任务为代表
        String dispatchType = taskExecutionRunnableList.get(0).getWorkflowInstance().getDispatchType();
        int dataRate = taskExecutionRunnableList.get(0).getWorkflowInstance().getDataRate();
        String workerGroup = taskExecutionRunnableList.get(0).getTaskExecutionContext().getWorkerGroup();
        tasksExecutionRunnable.setDispatchType(dispatchType);
        tasksExecutionRunnable.setWorkerGroup(workerGroup);

        // 获取任务信息
        BiMap<Integer, Long> taskBiMap = HashBiMap.create();
        int taskIndex = 0;
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            taskBiMap.put(taskIndex, taskExecutionRunnable.getTaskInstance().getTaskCode());
            taskIndex++;
        }
        tasksExecutionRunnable.setTaskExecutionRunnableList(taskExecutionRunnableList);
        tasksExecutionRunnable.setTaskBiMap(taskBiMap);

        // 获取带宽信息
        List<Bandwidth> bandwidthList = processService.findDataCenterBandwidth();
        Table<Host, Host, Double> bandwidthTable = HashBasedTable.create(); // a(j,j')
        for (Bandwidth bandwidth : bandwidthList) {
            bandwidthTable.put(Host.of(bandwidth.getPreHost()), Host.of(bandwidth.getPostHost()),
                    bandwidth.getBandwidth());
        }
        tasksExecutionRunnable.setBandwidthList(bandwidthList);
        tasksExecutionRunnable.setBandwidthTable(bandwidthTable);

        // 获取线程状态信息
        List<HostThreadStatus> allHostThreadStatusList = processService.queryHostThreadStatus();
        Map<Host, Integer> hostThreadNum = getHostThreadNum();
        List<HostThreadStatus> hostThreadStatusList = new ArrayList<>();
        Table<Host, Integer, Long> hostUsableTimeTable = HashBasedTable.create();
        for (HostThreadStatus hostThreadStatus : allHostThreadStatusList) {
            int threadNum = 0;
            if (hostThreadNum.get(Host.of(hostThreadStatus.getHost())) != null) {
                threadNum = hostThreadNum.get(Host.of(hostThreadStatus.getHost()));
                if (hostThreadStatus.getThreadId() < threadNum) {
                    hostThreadStatusList.add(hostThreadStatus);
                    hostUsableTimeTable.put(Host.of(hostThreadStatus.getHost()), hostThreadStatus.getThreadId(),
                            hostThreadStatus.getUsableTimeStamp());
                }
            }
        }
        tasksExecutionRunnable.setHostThreadNum(hostThreadNum);
        tasksExecutionRunnable.setHostThreadStatusList(hostThreadStatusList);
        tasksExecutionRunnable.setHostUsableTimeTable(hostUsableTimeTable);

        // 获取任务所需数据信息
        Table<Long, Long, IntermediateData> tasksNeedTasksData = getTasksNeedTasksData(taskExecutionRunnableList, dataRate);
        BiMap<Integer, Long> dataBiMap = HashBiMap.create();
        Map<Long, HostThread> dataHostThreadMap = new HashMap<>();
        int dataIndex = 0;
        for (long dataCode : tasksNeedTasksData.columnMap().keySet()) {
            dataBiMap.put(dataIndex, dataCode);
            dataIndex++;
        }
        for (Table.Cell<Long, Long, IntermediateData> cell : tasksNeedTasksData.cellSet()) {
            HostThread hostThread = new HostThread(Host.of(cell.getValue().getHost()), 0);
            dataHostThreadMap.put(cell.getColumnKey(), hostThread);
        }
        tasksExecutionRunnable.setTasksNeedTasksData(tasksNeedTasksData);
        tasksExecutionRunnable.setDataHostThreadMap(dataHostThreadMap);
        tasksExecutionRunnable.setDataBiMap(dataBiMap);

        // 预测各任务执行时间
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            long exeTime = predictExeTime(taskExecutionRunnable);
            taskInstance.setExeTime(DateUtils.format2DurationMS(exeTime));
        }
        return tasksExecutionRunnable;
    }

    public Table<Long, Long, IntermediateData> getTasksNeedTasksData(@NonNull List<ITaskExecutionRunnable> taskExecutionRunnableList, int dataRate) {
        Table<Long, Long, IntermediateData> taskDataTable = HashBasedTable.create();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            List<TaskInstance> leftTaskInstanceList = getLeftTaskInstanceList(taskExecutionRunnable);
            for (TaskInstance leftTaskInstance : leftTaskInstanceList) {
                long leftTaskCode = leftTaskInstance.getTaskCode();
                String host = leftTaskInstance.getDataHost();
                String dataStr = leftTaskInstance.getOutputDataSize();
                long dataSize = Long.parseLong(dataStr.substring(0, dataStr.length() - 1));
                IntermediateData data = new IntermediateData(leftTaskInstance, host, dataSize);
                taskDataTable.put(taskInstance.getTaskCode(), leftTaskCode, data);
            }
        }
        return taskDataTable;
    }

    public List<TaskInstance> getLeftTaskInstanceList(@NonNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        // 获取工作流定义的任务依赖关系
        List<WorkflowTaskRelation> relationList =
                processService.findRelationByCode(ITaskExecutionRunnable.getWorkflowDefinition().getCode(),
                        ITaskExecutionRunnable.getWorkflowDefinition().getVersion());

        // 获取当前任务的所有前置任务
        Long code = ITaskExecutionRunnable.getTaskDefinition().getCode();
        List<Long> leftTaskCodeList = relationList.stream()
                .filter(relation -> code.equals(relation.getPostTaskCode()))
                .map(WorkflowTaskRelation::getPreTaskCode)
                .filter(preTaskCode -> preTaskCode != 0)
                .collect(Collectors.toList());

        // 如果没有前置任务，就返回空值
        if (leftTaskCodeList.isEmpty()) {
            return new ArrayList<>();
        }

        // 获取当前工作流实例关联的任务实例
        List<TaskInstance> currentTaskInstanceList =
                processService
                        .findTaskInstanceByWorkflowInstanceId(ITaskExecutionRunnable.getWorkflowInstance().getId());

        // 获取前置节点跑完的任务实例
        List<TaskInstance> leftTaskInstanceList = currentTaskInstanceList.stream()
                .filter(taskInstance -> leftTaskCodeList.contains(taskInstance.getTaskCode()))
                .filter(taskInstance -> taskInstance.getState().getCode() == 7) // 执行成功
                .collect(Collectors.toList());

        return leftTaskInstanceList;
    }

    public long predictExeTime(@NonNull ITaskExecutionRunnable ITaskExecutionRunnable) {
        TaskInstance taskInstance = ITaskExecutionRunnable.getTaskInstance();
        // 获取往期任务实例
        List<TaskInstance> oldTaskInstanceList = processService.querySuccessTaskInstanceByTaskCode(
                taskInstance.getTaskCode(), taskInstance.getTaskDefinitionVersion(), 10);

        // 用于存储所有的执行时间值
        List<Long> durationList = new ArrayList<>();
        // 求每个任务实例的持续时间并添加到列表中
        for (TaskInstance task : oldTaskInstanceList) {
            Long duration = DateUtils.duration2MS(task.getDuration()); // 单位ms
            durationList.add(duration);
        }

        // 计算平均值
        double sum = 0;
        for (Long duration : durationList) {
            sum += duration;
        }

        if (durationList.isEmpty())
            return 0;
        else
            return (long) (sum / durationList.size());
    }

    protected void setTasksInstanceInfo(TasksExecutionRunnable tasksExecutionRunnable,
                                        Map<Long, HostThread> tasksDispatchHostThread, long dispatchTime) {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        long dispatchCode = tasksExecutionRunnable.getDispatchCode();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            HostThread hostThread = tasksDispatchHostThread.get(taskInstance.getTaskCode());
            taskInstance.setThreadId(hostThread.getThreadId());
            taskInstance.setDispatchTime(DateUtils.format2DurationMS(dispatchTime));
            taskInstance.setDispatchCode(dispatchCode);
        }
        setWaitTime(tasksExecutionRunnable, tasksDispatchHostThread);
        setTransferTime(tasksExecutionRunnable, tasksDispatchHostThread);
        setCompletionTime(tasksExecutionRunnable);
    }

    protected void setWaitTime(TasksExecutionRunnable tasksExecutionRunnable,
                               Map<Long, HostThread> tasksDispatchHostThread) {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        Table<Host, Integer, Long> hostUsableTimeTable = tasksExecutionRunnable.getHostUsableTimeTable();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            HostThread hostThread = tasksDispatchHostThread.get(taskInstance.getTaskCode());
            Host host = hostThread.getHost();
            int threadId = hostThread.getThreadId();
            long usableTime = hostUsableTimeTable.get(host, threadId);
            long waitTime = usableTime - taskInstance.getSubmitTime().getTime();
            if (waitTime < 0) {
                waitTime = 0;
            }
            // System.out.println("任务" + taskInstance.getName() + "放在" + host + "线程" + threadId + "的等待时间是：" + waitTime +
            // " --- " + usableTime);
            taskInstance.setWaitTime(DateUtils.format2DurationMS(waitTime));
        }
    }

    protected void setTransferTime(TasksExecutionRunnable tasksExecutionRunnable,
                                   Map<Long, HostThread> tasksDispatchHostThread) {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        Table<Host, Host, Double> bandwidthTable = tasksExecutionRunnable.getBandwidthTable();
        Table<Long, Long, IntermediateData> tasksNeedTasksData = tasksExecutionRunnable.getTasksNeedTasksData();
        Map<Long, HostThread> dataHostThreadMap = tasksExecutionRunnable.getDataHostThreadMap();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            long transferTime = 0;
            long receiveDataSize = 0;
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            HostThread hostThread = tasksDispatchHostThread.get(taskInstance.getTaskCode());
            Host taskHost = hostThread.getHost();
            Map<Long, IntermediateData> dataMap = tasksNeedTasksData.rowMap().get(taskInstance.getTaskCode());
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
                    if (!dataHost.getAddress().equals(taskHost.getAddress())) {
                        receiveDataSize += dataSize;
                    }
                }
            }
            taskInstance.setTransferTime(DateUtils.format2DurationMS(transferTime));
            taskInstance.setReceiveDataSize(receiveDataSize + "B");
        }
    }

    protected void setCompletionTime(TasksExecutionRunnable tasksExecutionRunnable) {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            long waitTime = DateUtils.duration2MS(taskInstance.getWaitTime());
            long transferTime = DateUtils.duration2MS(taskInstance.getTransferTime());
            long exeTime = DateUtils.duration2MS(taskInstance.getExeTime());
            long completionTime = Math.max(waitTime, transferTime) + exeTime;
            float ru = 100;
            if (transferTime > waitTime) {
                long tw = transferTime - waitTime;
                if (exeTime != 0) {
                    ru = (float) exeTime / (exeTime + tw) * 100;
                }
            }
            String resourceUtilization = String.format("%.2f", ru) + "%";
            taskInstance.setCompletionTime(DateUtils.format2DurationMS(completionTime));
            taskInstance.setResourceUtilization(resourceUtilization);
        }
    }

    protected void setHostsThreadUsableTime(TasksExecutionRunnable tasksExecutionRunnable,
                                            Map<Long, HostThread> tasksDispatchHostThread) {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = tasksExecutionRunnable.getTaskExecutionRunnableList();
        List<HostThreadStatus> hostThreadStatusList = tasksExecutionRunnable.getHostThreadStatusList();
        for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
            TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            HostThread hostThread = tasksDispatchHostThread.get(taskInstance.getTaskCode());
            String host = hostThread.getHost().getAddress();
            int threadId = hostThread.getThreadId();
            HostThreadStatus oldHostThreadStatus = hostThreadStatusList.stream()
                    .filter(v -> host.equals(v.getHost()) && threadId == v.getThreadId())
                    .findFirst().get();

            long transferTime = DateUtils.duration2MS(taskInstance.getTransferTime());
            long exeTime = DateUtils.duration2MS(taskInstance.getExeTime());
            long transferEndTime = taskInstance.getSubmitTime().getTime() + transferTime; // 传输时间点
            long usableTime = Math.max(oldHostThreadStatus.getUsableTimeStamp(), transferEndTime) + exeTime; // 可用时间点

            HostThreadStatus hostThreadStatus = new HostThreadStatus();
            hostThreadStatus.setHost(host);
            hostThreadStatus.setThreadId(threadId);
            hostThreadStatus.setId(oldHostThreadStatus.getId());
            hostThreadStatus.setUsableTime(new Date(usableTime));
            hostThreadStatus.setUsableTimeStamp(usableTime);
            processService.updateThreadUsableTime(hostThreadStatus);
        }
    }

    @SneakyThrows // 简化异常处理
    public Map<Host, Long> getHostData(@NonNull ITaskExecutionRunnable ITaskExecutionRunnable, int dataRate) {
        // 获取前置节点任务实例
        List<TaskInstance> leftTaskInstanceList = getLeftTaskInstanceList(ITaskExecutionRunnable);

        // 整合各host的数据量
        Map<Host, Long> hostDataMap = new HashMap<>();
        for (TaskInstance taskInstance : leftTaskInstanceList) {
            Host host = Host.of(taskInstance.getHost());
            String dataStr = taskInstance.getOutputDataSize();
            // 求出数据大小，单位比特
            long dataSize = Long.parseLong(dataStr.substring(0, dataStr.length() - 1));
            if (hostDataMap.get(host) != null) {
                dataSize += hostDataMap.get(host);
            }
            hostDataMap.put(host, dataSize);
        }

        return hostDataMap;
    }

    protected long setTaskTransferTime(ITaskExecutionRunnable ITaskExecutionRunnable,
                                       Optional<HostThread> optionalHostThread) {
        TaskInstance taskInstance = ITaskExecutionRunnable.getTaskInstance();
        long transferTime = 0;
        long receiveDataSize = 0;
        Map<Host, Long> hostDataMap = ITaskExecutionRunnable.getTaskNeedHostData();
        for (Map.Entry<Host, Long> entry : hostDataMap.entrySet()) {
            String preHost = entry.getKey().getAddress();
            String postHost = optionalHostThread.get().getHost().getAddress();
            double bandwidth = ITaskExecutionRunnable.getBandwidthList().stream()
                    .filter(v -> preHost.equals(v.getPreHost()) && postHost.equals(v.getPostHost()))
                    .map(Bandwidth::getBandwidth)
                    .findFirst().get();
            long dataSize = entry.getValue();
            long localTransferTime = (long) (dataSize * 1000 / bandwidth); // 单位ms
            if (localTransferTime >= transferTime) {
                transferTime = localTransferTime;
            }
            if (!preHost.equals(postHost)) {
                receiveDataSize += dataSize;
            }
        }
        taskInstance.setTransferTime(DateUtils.format2DurationMS(transferTime));
        taskInstance.setReceiveDataSize(receiveDataSize + "B");
        return transferTime;
    }

    protected long setWaitTime(ITaskExecutionRunnable ITaskExecutionRunnable, Optional<HostThread> optionalHostThread) {
        TaskInstance taskInstance = ITaskExecutionRunnable.getTaskInstance();
        HostThread hostThread = optionalHostThread.get();

        Table<Host, Integer, Long> hostUsableTimeTable = HashBasedTable.create();
        for (HostThreadStatus hostThreadStatus : ITaskExecutionRunnable.getHostThreadStatusList()) {
            hostUsableTimeTable.put(Host.of(hostThreadStatus.getHost()), hostThreadStatus.getThreadId(),
                    hostThreadStatus.getUsableTime().getTime());
        }
        Host host = hostThread.getHost();
        int threadId = hostThread.getThreadId();
        long usableTime = ITaskExecutionRunnable.getHostThreadStatusList().stream()
                .filter(v -> host.equals(Host.of(v.getHost())) && threadId == v.getThreadId())
                .map(v -> v.getUsableTime().getTime())
                .findFirst().get();
        long waitTime = usableTime - taskInstance.getSubmitTime().getTime();
        if (waitTime < 0) {
            waitTime = 0;
        }
        taskInstance.setWaitTime(DateUtils.format2DurationMS(waitTime));
        return waitTime;
    }

    protected void setHostThreadUsableTime(ITaskExecutionRunnable ITaskExecutionRunnable, HostThread hostThread,
                                           long transferTime, long exeTime) {

        TaskInstance taskInstance = ITaskExecutionRunnable.getTaskInstance();

        // 更新host线程可用时间
        String hostAddress = hostThread.getHost().getAddress();
        int threadId = hostThread.getThreadId();
        HostThreadStatus oldHostThreadStatus = ITaskExecutionRunnable.getHostThreadStatusList().stream()
                .filter(v -> hostAddress.equals(v.getHost()) && threadId == v.getThreadId())
                .findFirst().get();

        // 计算可用时间点
        long transferEndTime = taskInstance.getSubmitTime().getTime() + transferTime; // 传输时间点
        long usableTime = Math.max(oldHostThreadStatus.getUsableTimeStamp(), transferEndTime) + exeTime; // 可用时间点

        // 更新数据库
        HostThreadStatus hostThreadStatus = new HostThreadStatus();
        hostThreadStatus.setHost(hostAddress);
        hostThreadStatus.setThreadId(threadId);
        hostThreadStatus.setId(oldHostThreadStatus.getId());
        hostThreadStatus.setUsableTime(new Date(usableTime));
        hostThreadStatus.setUsableTimeStamp(usableTime);
        processService.updateThreadUsableTime(hostThreadStatus);

    }

    protected void updateDispatcherHistory(ITaskExecutionRunnable ITaskExecutionRunnable, Optional<Host> optionalHost) {
        // 获取工作流的分发历史记录
        Map<TaskDefinition, Host> taskHostMap = DispatcherHistory.getCurrentTaskHostMap();

        // 如果开始新工作流，则先清空
        for (Map.Entry<TaskDefinition, Host> entry : taskHostMap.entrySet()) {
            long taskCode = entry.getKey().getCode();
            String locations = ITaskExecutionRunnable.getWorkflowDefinition().getLocations();
            // 当前工作流定义中的locations是否包含之前的任务编码
            if (!locations.contains(String.valueOf(taskCode))) {
                taskHostMap = new HashMap<>();
            }
            break;
        }

        // 获取任务id和任务分发host
        TaskDefinition taskDefinition = ITaskExecutionRunnable.getTaskDefinition();
        Host taskHost = optionalHost.get();

        // 更新工作流的分发历史记录
        taskHostMap.put(taskDefinition, taskHost);
        DispatcherHistory.setCurrentTaskHostMap(taskHostMap);

        Map<TaskDefinition, Host> old = DispatcherHistory.getCurrentTaskHostMap();
    }
}

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

package org.apache.dolphinscheduler.server.master.engine.workflow.statemachine;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.WorkflowCacheRepository;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBusCoordinator;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.SuccessorFlowAdjuster;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowFinalizeLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.utils.WorkflowInstanceUtils;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@Slf4j
public abstract class AbstractWorkflowStateAction implements IWorkflowStateAction {

    @Autowired
    protected SuccessorFlowAdjuster successorFlowAdjuster;

    @Autowired
    protected WorkflowInstanceDao workflowInstanceDao;

    @Autowired
    protected TaskInstanceDao taskInstanceDao;

    @Autowired
    protected WorkflowCacheRepository workflowCacheRepository;

    @Autowired
    protected WorkflowEventBusCoordinator workflowEventBusCoordinator;

    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    /**
     * Try to trigger the tasks if the trigger condition is met.
     * <p> If all the given tasks trigger condition is not met then will try to emit workflow finish event.
     */
    protected void triggerTasks(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final List<ITaskExecutionRunnable> taskExecutionRunnables) {
        final IWorkflowExecutionGraph workflowExecutionGraph = workflowExecutionRunnable.getWorkflowExecutionGraph();
        final List<ITaskExecutionRunnable> readyTaskExecutionRunnableList = taskExecutionRunnables
                .stream()
                .filter(workflowExecutionGraph::isTriggerConditionMet)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(readyTaskExecutionRunnableList)) {
            emitWorkflowFinishedEventIfApplicable(workflowExecutionRunnable);
            return;
        }
        final WorkflowEventBus workflowEventBus = workflowExecutionRunnable.getWorkflowEventBus();
        for (ITaskExecutionRunnable readyTaskExecutionRunnable : readyTaskExecutionRunnableList) {
            workflowExecutionGraph.markTaskExecutionRunnableActive(readyTaskExecutionRunnable);
            if (workflowExecutionGraph.isTaskExecutionRunnableSkipped(readyTaskExecutionRunnable)
                    || workflowExecutionGraph.isTaskExecutionRunnableForbidden(readyTaskExecutionRunnable)) {
                workflowExecutionGraph.markTaskExecutionRunnableInActive(readyTaskExecutionRunnable);
                workflowEventBus.publish(
                        WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent.of(
                                workflowExecutionRunnable, readyTaskExecutionRunnable));
                continue;
            }
            // 判断是否存在同名且类型为TaskStartLifecycleEvent的任务在delayEventQueue中
            boolean shouldPublish = workflowEventBus.getDelayEventQueue().stream()
                    .filter(delayEvent -> delayEvent.getEventType() == TaskLifecycleEventType.START)
                    .map(delayEvent -> {
                        TaskStartLifecycleEvent event = (TaskStartLifecycleEvent) delayEvent;
                        return event.getTaskExecutionRunnable().getName();
                    })
                    .noneMatch(name -> name.equals(readyTaskExecutionRunnable.getName()));

            if (shouldPublish) {
                workflowEventBus.publish(TaskStartLifecycleEvent.of(readyTaskExecutionRunnable));
            }
        }
    }

    protected void onTaskFinish(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final ITaskExecutionRunnable taskExecutionRunnable) {
        final IWorkflowExecutionGraph workflowExecutionGraph = workflowExecutionRunnable.getWorkflowExecutionGraph();
        if (workflowExecutionGraph.isEndOfTaskChain(taskExecutionRunnable)) {
            emitWorkflowFinishedEventIfApplicable(workflowExecutionRunnable);
            return;
        }

        successorFlowAdjuster.adjustSuccessorFlow(taskExecutionRunnable);
        // 处理给定任务的后续任务
        triggerTasks(workflowExecutionRunnable, workflowExecutionGraph.getSuccessors(taskExecutionRunnable));
    }

    protected String getTransferDataSize(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        // 获取当前工作流实例关联的任务实例
        List<TaskInstance> currentTaskInstanceList =
                processService.findTaskInstanceByWorkflowInstanceId(workflowInstance.getId());
        long receiveDataSize = 0;
        for (TaskInstance taskInstance : currentTaskInstanceList) {
            String size = taskInstance.getReceiveDataSize();
            receiveDataSize += Long.parseLong(size.substring(0, size.length() - 1));
        }
        return receiveDataSize + "B";
    }

    protected String getResourceUtilization(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        // 获取当前工作流实例关联的任务实例
        List<TaskInstance> currentTaskInstanceList =
                processService.findTaskInstanceByWorkflowInstanceId(workflowInstance.getId());
        float ru = 0;
        int len = 0;
        for (TaskInstance taskInstance : currentTaskInstanceList) {
            String taskRU = taskInstance.getResourceUtilization();
            float num = Float.parseFloat(taskRU.substring(0, taskRU.length() - 1));
            ru += num;
            len += 1;
        }
        ru /= len;
        return String.format("%.2f", ru) + "%";
    }

    public long predictIntervalTime(TaskInstance left, TaskInstance right) {
        // 获取往期任务实例
        List<TaskInstance> oldLeftTaskInstanceList = processService.querySuccessTaskInstanceByTaskCode(
                left.getTaskCode(), left.getTaskDefinitionVersion(), 10);
        List<TaskInstance> oldRightTaskInstanceList = processService.querySuccessTaskInstanceByTaskCode(
                right.getTaskCode(), right.getTaskDefinitionVersion(), 10);
        List<Integer> workflowInstanceIds = new ArrayList<>();
        Map<Integer, TaskInstance> workflowLeftTasks = new HashMap<>();
        for (TaskInstance task : oldLeftTaskInstanceList) {
            workflowLeftTasks.put(task.getWorkflowInstanceId(), task);
            workflowInstanceIds.add(task.getWorkflowInstanceId());
        }
        Map<Integer, TaskInstance> workflowRightTasks = new HashMap<>();
        for (TaskInstance task : oldRightTaskInstanceList) {
            workflowRightTasks.put(task.getWorkflowInstanceId(), task);
            workflowInstanceIds.add(task.getWorkflowInstanceId());
        }

        // 用于存储所有的时间值
        List<Long> durationList = new ArrayList<>();
        // 求每个任务实例的持续时间并添加到列表中
        for (Integer id : workflowInstanceIds) {
            if (workflowLeftTasks.containsKey(id) && workflowRightTasks.containsKey(id)) {
                long startTime = workflowLeftTasks.get(id).getEndTime().getTime();
                if (workflowRightTasks.get(id).getStartTime() != null) {
                    long endTime = workflowRightTasks.get(id).getStartTime().getTime();
                    long time = endTime - startTime;
                    if (time < 0)
                        time = 0;
                    durationList.add(time);
                }
            }
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

    protected long getCompletionTime(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        // 获取工作流定义的任务依赖关系
        List<WorkflowTaskRelation> relationList =
                processService.findRelationByCode(workflowInstance.getWorkflowDefinitionCode(),
                        workflowInstance.getWorkflowDefinitionVersion());
        // 获取当前工作流实例关联的任务实例
        List<TaskInstance> currentTaskInstanceList =
                processService.findTaskInstanceByWorkflowInstanceId(workflowInstance.getId());
        // 定义每个任务的完成时间，初始化为运行时间
        Map<Long, Long> taskCompletionTimeMap = new HashMap<>();
        // dp(u)表示从u开始一直到连接的最后节点之间的时间
        Map<Long, Long> dp = new HashMap<>();
        Map<Long, Queue<Long>> paths = new HashMap<>();
        Map<Long, TaskInstance> taskInstanceMap = new HashMap<>();
        // 定义邻接表(u的后继节点v集合)
        Map<Long, Vector<Long>> taskAdjMap = new HashMap<>();
        // 定义每个任务的入度，初始化为0
        Map<Long, Integer> taskInDegreeMap = new HashMap<>();
        // Map<Long, Long> endTimeMap = new HashMap<>();
        // Map<Long, Long> startTimeMap = new HashMap<>();
        for (TaskInstance taskInstance : currentTaskInstanceList) {
            long taskCode = taskInstance.getTaskCode();
            taskInstanceMap.put(taskCode, taskInstance);
            long waitTime = DateUtils.duration2MS(taskInstance.getWaitTime());
            long transferTime = DateUtils.duration2MS(taskInstance.getTransferTime());
            long time = Math.max(waitTime, transferTime);
            taskCompletionTimeMap.put(taskCode, time);
            dp.put(taskCode, DateUtils.duration2MS(taskInstance.getCompletionTime()));
            paths.put(taskCode, new LinkedList<>());
            paths.get(taskCode).add(taskCode);
            taskInDegreeMap.put(taskCode, 0);
            taskAdjMap.put(taskCode, new Vector<>());
            // endTimeMap.put(taskCode, taskInstance.getEndTime().getTime());
            // startTimeMap.put(taskCode, taskInstance.getStartTime().getTime());
        }
        // 计算每个任务的入度和邻接表
        for (WorkflowTaskRelation relation : relationList) {
            if (relation.getPreTaskCode() != 0 && relation.getPostTaskCode() != 0) {
                taskInDegreeMap.merge(relation.getPostTaskCode(), 1, Integer::sum);
                taskAdjMap.get(relation.getPreTaskCode()).add(relation.getPostTaskCode());
            }
        }
        // 定义拓扑排序后的任务列表
        List<Long> taskOrderedList = new ArrayList<>();
        Queue<Long> queue = new LinkedList<>();
        for (Map.Entry<Long, Integer> entry : taskInDegreeMap.entrySet()) {
            if (entry.getValue() == 0)
                queue.add(entry.getKey()); // 所有入度为0的节点加入队列
        }
        while (!queue.isEmpty()) {
            // 取出队首顶点放入排序列表
            long u = queue.remove();
            taskOrderedList.add(u);
            // 所有后继节点的入度减一，减到0的加入队列
            for (long v : taskAdjMap.get(u)) {
                taskInDegreeMap.merge(v, -1, Integer::sum);
                if (taskInDegreeMap.get(v) == 0)
                    queue.add(v);
            }
        }
        // 动态规划计算最大路径
        Queue<Long> keyPath = new LinkedList<>();
        long complentionTime = 0;
        // 从后面开始
        for (int i = taskOrderedList.size() - 1; i >= 0; i--) {
            long u = taskOrderedList.get(i);
            Queue<Long> uPath = paths.get(u);
            if (taskAdjMap.get(u).isEmpty()) {
                dp.put(u, taskCompletionTimeMap.get(u));
            }
            Long next = null;
            for (long v : taskAdjMap.get(u)) {
                // long intervalTime = startTimeMap.get(v) - endTimeMap.get(u);
                // if (intervalTime < 0) intervalTime = 0;
                long intervalTime = predictIntervalTime(taskInstanceMap.get(u), taskInstanceMap.get(v));
                long time = dp.get(v) + taskCompletionTimeMap.get(u);
                if (time > dp.get(u)) {
                    dp.put(u, time);
                    next = v;
                }
            }
            if (next != null) {
                Queue<Long> vPath = paths.get(next);
                uPath.addAll(vPath);
            }
            if (dp.get(u) > complentionTime) {
                complentionTime = dp.get(u);
                keyPath = paths.get(u);
            }
        }

        // 更新关键路径上的节点标记
        List<Long> taskList = new ArrayList<>(keyPath);
        for (int i = 0; i < taskList.size(); i++) {
            TaskInstance taskInstance = taskInstanceMap.get(taskList.get(i));
            taskInstance.setKeyFlag(1);
            if (i < taskList.size() - 1) {
                taskInstance.setNextKeyTask(taskList.get(i + 1));
            }
            processService.updateTaskInstance(taskInstance);
        }
        log.info("关键路径是 {}", keyPath);
        return complentionTime;
    }

    protected void workflowFinish(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                  final WorkflowExecutionStatus workflowExecutionStatus) {
        final WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        workflowInstance.setEndTime(new Date());
        workflowInstance.setState(workflowExecutionStatus);
        long duration = workflowInstance.getEndTime().getTime() - workflowInstance.getStartTime().getTime();
        long completionTime = duration + getCompletionTime(workflowExecutionRunnable);
        workflowInstance.setCompletionTime(DateUtils.format2DurationMS(completionTime));
        workflowInstance.setTransferDataSize(getTransferDataSize(workflowExecutionRunnable));
        workflowInstance.setResourceUtilization(getResourceUtilization(workflowExecutionRunnable));
        transformWorkflowInstanceState(workflowExecutionRunnable, workflowExecutionStatus);
        workflowExecutionRunnable.getWorkflowEventBus()
                .publish(WorkflowFinalizeLifecycleEvent.of(workflowExecutionRunnable));
    }

    /**
     * Transformer the workflow instance state to targetState. This method will both update the
     * workflow instance state in memory and in the database.
     */
    protected void transformWorkflowInstanceState(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                                  final WorkflowExecutionStatus targetState) {
        final WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        WorkflowExecutionStatus originState = workflowInstance.getState();
        try {
            workflowInstance.setState(targetState);
            workflowInstanceDao.updateById(workflowInstance);
            log.info("数据库更新工作流实例结束时间和状态!!!");
            log.info("Success set WorkflowExecuteRunnable: {} state from: {} to {}",
                    workflowInstance.getName(), originState.name(), targetState.name());

            // 更新分发索引
            List<TaskInstance> taskInstanceList =
                    processService.findTaskInstanceByWorkflowInstanceId(workflowInstance.getId()); // 获取当前工作流实例关联的任务实例
            List<Long> sortedUniqueDispatchCodes = taskInstanceList.stream()
                    .map(TaskInstance::getDispatchCode)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            BiMap<Integer, Long> dispatchBiMap = HashBiMap.create();
            int index = 0;
            for (long code : sortedUniqueDispatchCodes) {
                dispatchBiMap.put(index, code);
                index++;
            }
            for (TaskInstance taskInstance : taskInstanceList) {
                long dispatchCode = taskInstance.getDispatchCode();
                taskInstance.setDispatchIndex(dispatchBiMap.inverse().get(dispatchCode));
                taskInstanceDao.updateById(taskInstance);
            }

        } catch (Exception ex) {
            workflowInstance.setState(originState);
            throw ex;
        }
    }

    /**
     * Emit the workflow finished event if the workflow can finish, otherwise do nothing.
     * <p> The workflow finish state is determined by the state of the task in the workflow.
     */
    protected abstract void emitWorkflowFinishedEventIfApplicable(final IWorkflowExecutionRunnable workflowExecutionRunnable);

    /**
     * Assert that the state of the task is the expected state.
     *
     * @throws IllegalStateException if the state of the task is not the expected state.
     */
    protected void throwExceptionIfStateIsNotMatch(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        checkNotNull(workflowExecutionRunnable, "workflowExecutionRunnable is null");
        final WorkflowExecutionStatus actualState = workflowExecutionRunnable.getState();
        final WorkflowExecutionStatus expectState = matchState();
        if (actualState != expectState) {
            final String workflowName = workflowExecutionRunnable.getName();
            throw new IllegalStateException(
                    "The workflow: " + workflowName + " state: " + actualState + " is not match:" + expectState);
        }
    }

    protected void logWarningIfCannotDoAction(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                              final AbstractLifecycleEvent event) {
        log.warn("Workflow {} state is {} cannot do action on event: {}",
                workflowExecutionRunnable.getName(),
                workflowExecutionRunnable.getState(),
                event);
    }

    protected void finalizeEventAction(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        log.info(WorkflowInstanceUtils.logWorkflowInstanceInDetails(workflowExecutionRunnable));

        workflowCacheRepository.remove(workflowExecutionRunnable.getId());
        workflowEventBusCoordinator.unRegisterWorkflowEventBus(workflowExecutionRunnable);

        log.info("Successfully finalize WorkflowExecuteRunnable: {}", workflowExecutionRunnable.getName());
    }
}

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

package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.runner.dispatcher.TaskDispatchFactory;
import org.apache.dolphinscheduler.server.master.runner.dispatcher.TaskDispatcher;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GlobalTaskDispatchWaitingQueueLooper extends BaseDaemonThread implements AutoCloseable {

    private final long intervalTime = 100; // 间隔时间100ms

    private long lastUpdateTime = System.currentTimeMillis(); // 上次更新时间

    private Queue<ITaskExecutionRunnable> taskDispatchAblePool = new LinkedList<>(); // 可调度池

    @Autowired
    private GlobalTaskDispatchWaitingQueue globalTaskDispatchWaitingQueue;

    @Autowired
    private TaskDispatchFactory taskDispatchFactory;

    private final AtomicBoolean RUNNING_FLAG = new AtomicBoolean(false);

    public GlobalTaskDispatchWaitingQueueLooper() {
        super("GlobalTaskDispatchWaitingQueueLooper");
    }

    @Override
    public synchronized void start() {
        if (!RUNNING_FLAG.compareAndSet(false, true)) {
            log.error("The GlobalTaskDispatchWaitingQueueLooper already started, will not start again");
            return;
        }
        log.info("GlobalTaskDispatchWaitingQueueLooper starting...");
        super.start();
        log.info("GlobalTaskDispatchWaitingQueueLooper started...");
    }

    @Override
    public void run() {
        while (RUNNING_FLAG.get()) {

            long currentTime = System.currentTimeMillis();

            // delay队列确保取出延迟时间过期的，take方法取不到会一直阻塞
            // ITaskExecutionRunnable taskExecutionRunnable = globalTaskDispatchWaitingQueue.takeTaskExecuteRunnable();

            // poll方法取不到会返回null
            ITaskExecutionRunnable taskExecutionRunnable = globalTaskDispatchWaitingQueue.pollTaskExecuteRunnable();
            if(taskExecutionRunnable != null) {
                String taskType = taskExecutionRunnable.getTaskInstance().getTaskType();
                String dispatchType = taskExecutionRunnable.getWorkflowInstance().getDispatchType();
                if (TaskTypeUtils.isLogicTask(taskType) || !isMultiDispatcher(dispatchType)) { // 如果为逻辑任务或不支持多任务调度则直接分发
                    doOneDispatch(taskExecutionRunnable);
                } else {
                    taskDispatchAblePool.add(taskExecutionRunnable); // 加入可调度池
                    lastUpdateTime = currentTime;
                }
            }

            // 如果超出间隔时间就执行分发
            if(currentTime - lastUpdateTime > intervalTime) {
                if(!taskDispatchAblePool.isEmpty()) doMultiDispatch();
            }
        }
    }

    void doMultiDispatch() {
        List<ITaskExecutionRunnable> taskExecutionRunnableList = new ArrayList<>();
        while (!taskDispatchAblePool.isEmpty()) {
            ITaskExecutionRunnable taskExecutionRunnable = taskDispatchAblePool.poll(); // 取出
            final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
            final TaskExecutionStatus status = taskInstance.getState();
            if (status != TaskExecutionStatus.SUBMITTED_SUCCESS && status != TaskExecutionStatus.DELAY_EXECUTION) {
                log.warn("The TaskInstance {} state is : {}, will not dispatch", taskInstance.getName(), status);
            } else {
                taskExecutionRunnableList.add(taskExecutionRunnable); // 加入调度集合
            }
        }
        try {
            final TaskDispatcher taskDispatcher = taskDispatchFactory.getWorkerTaskDispatcher();
            taskDispatcher.dispatchTasks(taskExecutionRunnableList);
//            if(taskExecutionRunnableList.size()==1) {
//                taskDispatcher.dispatchTask(taskExecutionRunnableList.get(0));
//            } else {
//                taskDispatcher.dispatchTasks(taskExecutionRunnableList);
//            }
        } catch (Exception e) {
            for (ITaskExecutionRunnable taskExecutionRunnable : taskExecutionRunnableList) {
                long waitingTimeMills = Math.max(taskExecutionRunnable.getTaskExecutionContext().increaseDispatchFailTimes() * 1_000L, 60_000L);
                globalTaskDispatchWaitingQueue.dispatchTaskExecuteRunnableWithDelay(taskExecutionRunnable, waitingTimeMills);
                log.error("Dispatch Task: {} failed will retry after: {}/ms", taskExecutionRunnable.getTaskInstance().getName(), waitingTimeMills, e);
            }
        }
    }

    void doDispatch() {
        final ITaskExecutionRunnable taskExecutionRunnable = globalTaskDispatchWaitingQueue.takeTaskExecuteRunnable();
        doOneDispatch(taskExecutionRunnable);
    }

    void doOneDispatch(ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        try {
            final TaskExecutionStatus status = taskInstance.getState();
            if (status != TaskExecutionStatus.SUBMITTED_SUCCESS && status != TaskExecutionStatus.DELAY_EXECUTION) {
                log.warn("The TaskInstance {} state is : {}, will not dispatch", taskInstance.getName(), status);
                return;
            }
            final TaskDispatcher taskDispatcher = taskDispatchFactory.getTaskDispatcher(taskInstance);
            taskDispatcher.dispatchTask(taskExecutionRunnable);
        } catch (Exception e) {
            // If dispatch failed, will put the task back to the queue
            // The task will be dispatched after waiting time.
            // the waiting time will increase multiple of times, but will not exceed 60 seconds
            long waitingTimeMills = Math.max(
                    taskExecutionRunnable.getTaskExecutionContext().increaseDispatchFailTimes() * 1_000L, 60_000L);
            globalTaskDispatchWaitingQueue.dispatchTaskExecuteRunnableWithDelay(taskExecutionRunnable,
                    waitingTimeMills);
            log.error("Dispatch Task: {} failed will retry after: {}/ms", taskInstance.getName(), waitingTimeMills, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (RUNNING_FLAG.compareAndSet(true, false)) {
            log.info("GlobalTaskDispatchWaitingQueueLooper stopping...");
            log.info("GlobalTaskDispatchWaitingQueueLooper stopped...");
        } else {
            log.error("GlobalTaskDispatchWaitingQueueLooper is not started");
        }
    }

    public boolean isMultiDispatcher(String dispatchType) {
        switch (dispatchType){
            case "RANDOM":
                return false;
            case "ROUND_ROBIN":
                return false;
            case "FIXED_WEIGHTED_ROUND_ROBIN":
                return false;
            case "DYNAMIC_WEIGHTED_ROUND_ROBIN":
                return false;
            case "WEIGHTED_RANDOM":
                return false;
            case "MAX_MIN_FAIRNESS":
                return true;
            case "FLUTTER":
                return true;
            case "TASK_DATA_SCHEDULE":
                return true;
            case "ON_LINE_TASK_SCHEDULE":
                return false;
            default:
                throw new IllegalArgumentException("不支持调度策略 " + dispatchType);
        }
    }
}

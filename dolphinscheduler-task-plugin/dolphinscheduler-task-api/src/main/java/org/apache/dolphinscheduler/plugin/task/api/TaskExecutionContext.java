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

package org.apache.dolphinscheduler.plugin.task.api;

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.api.resource.ResourceContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.beans.BeanUtils;

/**
 * to master/worker task transport
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskExecutionContext implements Serializable {

    private static final long serialVersionUID = -1L;

    private int taskInstanceId;

    private String taskName;

    private long firstSubmitTime;

    private long startTime;

    private String taskType;

    private String workflowInstanceHost;

    private String host;

    private String executePath;

    private String logPath;

    private String appInfoPath;

    private String taskJson;

    private int processId;

    private Long workflowDefinitionCode;

    private int workflowDefinitionVersion;

    private String appIds;

    private int workflowInstanceId;

    private long scheduleTime;

    private String globalParams;

    private int executorId;

    private int cmdTypeIfComplement;

    private String tenantCode;

    private int workflowDefinitionId;

    private int projectId;

    private long projectCode;

    private String taskParams;

    private String environmentConfig;

    /**
     * definedParams
     * // todo: we need to rename definedParams, prepareParamsMap, paramsMap, this is confusing
     */
    private Map<String, String> definedParams;

    private Map<String, Property> prepareParamsMap;

    private String taskAppId;

    private TaskTimeoutStrategy taskTimeoutStrategy;

    private int taskTimeout;

    private String workerGroup;

    private TaskExecutionStatus currentExecutionStatus;

    private ResourceParametersHelper resourceParametersHelper;

    private long endTime;

    private SQLTaskExecutionContext sqlTaskExecutionContext;

    private K8sTaskExecutionContext k8sTaskExecutionContext;

    private ResourceContext resourceContext;

    private String varPool;

    private String outResult;

    private int dryRun;

    private Map<String, Property> paramsMap;

    private DataQualityTaskExecutionContext dataQualityTaskExecutionContext;

    private Integer cpuQuota;

    private Integer memoryMax;

    private int testFlag;

    private boolean logBufferEnable;

    private int dispatchFailTimes;

    private boolean failover;

    public int increaseDispatchFailTimes() {
        return ++dispatchFailTimes;
    }

    // ssq 复制一个简易版taskExecutionContext，缩短过长的字符串，只作注释用
    public static TaskExecutionContext getMiniTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
        int miniSize = 200;
        TaskExecutionContext miniTaskExecutionContext = new TaskExecutionContext();
        BeanUtils.copyProperties(taskExecutionContext, miniTaskExecutionContext); // 浅拷贝，深拷贝无法使用
        // 缩短varpool的value
        if (taskExecutionContext.getVarPool() != null && taskExecutionContext.getVarPool().length() > miniSize) {
            miniTaskExecutionContext.setVarPool(taskExecutionContext.getVarPool().substring(0, miniSize) + "......]");
        }
        // 缩短prepareParamsMap里的property的value
        Map<String, Property> miniPrepareParamsMap = new HashMap<>();
        for (Map.Entry<String, Property> entry : taskExecutionContext.getPrepareParamsMap().entrySet()) {
            String key = entry.getKey();
            Property property = entry.getValue();
            if (property.getValue() != null && property.getValue().length() > miniSize) {
                Property miniProperty = new Property();
                BeanUtils.copyProperties(property, miniProperty); // 浅拷贝
                miniProperty.setValue(property.getValue().substring(0, miniSize) + "......]");
                miniPrepareParamsMap.put(key, miniProperty);
            } else {
                miniPrepareParamsMap.put(key, property);
            }
        }
        miniTaskExecutionContext.setPrepareParamsMap(miniPrepareParamsMap);
        return miniTaskExecutionContext;
    }
}

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

package org.apache.dolphinscheduler.api.utils;

import org.apache.dolphinscheduler.api.dto.DagDataSchedule;
import org.apache.dolphinscheduler.common.enums.ConditionType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.TimeoutFlag;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionTypeEnum;
import org.apache.dolphinscheduler.dao.entity.DagData;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

public class WorkflowTemplateUtils {

    public static DagDataSchedule getSelfWorkflow(long projectCode, String label, int num) {
        int taskNum = getTaskNum(label, num);
        String name = label + taskNum;
        // 设置工作流定义
        WorkflowDefinition workflowDefinition = getSelfWorkflowDefinition(projectCode, name, taskNum);
        // 设置任务关系
        List<WorkflowTaskRelation> workflowTaskRelationList = getSelfTaskRelations(projectCode, label, taskNum);
        // 设置任务定义
        List<TaskDefinition> taskDefinitionList = getSelfTaskDefinitionList(projectCode, taskNum);
        DagData dagData = new DagData(workflowDefinition, workflowTaskRelationList, taskDefinitionList);
        return new DagDataSchedule(dagData);
    }

    private static int getTaskNum(String label, int num) {
        int topNum;
        switch (label) {
            case "LIGO":
                if (num % 23 == 0)
                    return num;
                else
                    return (num / 23 + 1) * 23;
                // if ((num - 2) % 4 == 0)
                // topNum = (num - 2) / 4;
                // else
                // topNum = (num - 2) / 4 + 1;
                // return topNum * 4 + 2;
            case "Montage":
                if (num % 21 == 0)
                    return num;
                else
                    return (num / 21 + 1) * 21;
                // if ((num - 8) % 3 == 0)
                // topNum = (num - 8) / 3;
                // else
                // topNum = (num - 8) / 3 + 1;
                // return topNum * 3 + 8;
            case "CyberShake":
                if (num % 21 == 0)
                    return num;
                else
                    return (num / 21 + 1) * 21;
                // if ((num - 4) % 4 == 0)
                // topNum = (num - 4) / 4;
                // else
                // topNum = (num - 4) / 4 + 1;
                // return topNum * 4 + 4;
            case "AI":
                int t = (int) Math.sqrt(num);
                return t * t;
            default:
                return num;
        }
    }

    private static List<WorkflowTaskRelation> getSelfTaskRelations(long projectCode, String label, int num) {
        switch (label) {
            case "LIGO":
                return getManyLIGOTaskRelations(projectCode, num);
            case "Montage":
                return getManyMontageTaskRelations(projectCode, num);
            case "CyberShake":
                return getManyCyberShakeTaskRelations(projectCode, num);
            case "AI":
                return getManyAITaskRelations(projectCode, num);
            default:
                return getNoEdgeTaskRelations(projectCode, num);
        }
    }

    private static List<WorkflowTaskRelation> getManyAITaskRelations(long projectCode, int num) {
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        int t = (int) Math.sqrt(num);
        int id = 1;
        for (int i = 1; i <= num; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 0, 0, i, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = 0; i < t - 1; i++) {
            for (int j = 1; j <= t; j++) {
                int task = i * t + j;
                int nextI = i + 1;
                for (int k = 1; k <= t; k++) {
                    int nextTask = nextI * t + k;
                    WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                            projectCode, 0, task, 0, nextTask, 0,
                            ConditionType.NONE, "{}");
                    workflowTaskRelation.setId(id);
                    workflowTaskRelationList.add(workflowTaskRelation);
                    id++;
                }
            }
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getManyLIGOTaskRelations(long projectCode, int num) {
        int oneTaskNum = 23;
        int oneNum = num / oneTaskNum;
        int id = 1;
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        for (int i = 0; i < oneNum; i++) {
            List<WorkflowTaskRelation> list = getLIGOTaskRelations(projectCode, oneTaskNum - 1, id, i * oneTaskNum);
            workflowTaskRelationList.addAll(list);
            id = id + list.size();
            int mid = (i + 1) * oneTaskNum;
            for (int j = mid; j <= mid; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, 0, 0, j, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            for (int j = mid - 1; j <= mid - 1; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, j, 0, mid, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            if (i != oneNum - 1) {
                for (int j = mid + 1; j <= mid + 5; j++) {
                    WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                            projectCode, 0, mid, 0, j, 0,
                            ConditionType.NONE, "{}");
                    workflowTaskRelation.setId(id);
                    workflowTaskRelationList.add(workflowTaskRelation);
                    id++;
                }
            }
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getManyMontageTaskRelations(long projectCode, int num) {
        int oneTaskNum = 21;
        int oneNum = num / oneTaskNum;
        int id = 1;
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        for (int i = 0; i < oneNum; i++) {
            List<WorkflowTaskRelation> list = getMontageTaskRelations(projectCode, oneTaskNum - 1, id, i * oneTaskNum);
            workflowTaskRelationList.addAll(list);
            id = id + list.size();
            int mid = (i + 1) * oneTaskNum;
            for (int j = mid; j <= mid; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, 0, 0, j, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            for (int j = mid - 1; j <= mid - 1; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, j, 0, mid, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            if (i != oneNum - 1) {
                for (int j = mid + 1; j <= mid + 4; j++) {
                    WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                            projectCode, 0, mid, 0, j, 0,
                            ConditionType.NONE, "{}");
                    workflowTaskRelation.setId(id);
                    workflowTaskRelationList.add(workflowTaskRelation);
                    id++;
                }
            }
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getManyCyberShakeTaskRelations(long projectCode, int num) {
        int oneTaskNum = 21;
        int oneNum = num / oneTaskNum;
        int id = 1;
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        for (int i = 0; i < oneNum; i++) {
            List<WorkflowTaskRelation> list =
                    getCyberShakeTaskRelations(projectCode, oneTaskNum - 1, id, i * oneTaskNum);
            workflowTaskRelationList.addAll(list);
            id = id + list.size();
            int mid = (i + 1) * oneTaskNum;
            for (int j = mid; j <= mid; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, 0, 0, j, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            for (int j = mid - 1; j <= mid - 1; j++) {
                WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                        projectCode, 0, j, 0, mid, 0,
                        ConditionType.NONE, "{}");
                workflowTaskRelation.setId(id);
                workflowTaskRelationList.add(workflowTaskRelation);
                id++;
            }
            if (i != oneNum - 1) {
                for (int j = mid + 1; j <= mid + 2; j++) {
                    WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                            projectCode, 0, mid, 0, j, 0,
                            ConditionType.NONE, "{}");
                    workflowTaskRelation.setId(id);
                    workflowTaskRelationList.add(workflowTaskRelation);
                    id++;
                }
            }
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getNoEdgeTaskRelations(long projectCode, int num) {
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        for (int i = 1; i <= num; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 0, 0, i, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(i);
            workflowTaskRelationList.add(workflowTaskRelation);
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getLIGOTaskRelations(long projectCode, int num, int index, int offset) {
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        int topNum = (num - 2) / 4;
        int id = index;
        for (int i = 1; i <= num; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 0, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 1; i <= topNum * 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 1; i <= topNum * 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 2 + 1 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 2; i <= topNum * 3 + 1; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, topNum * 2 + 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 3 + 2; i <= topNum * 4 + 1; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 3 + 2; i <= topNum * 4 + 1; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 4 + 2 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getCyberShakeTaskRelations(long projectCode, int num, int index,
                                                                         int offset) {
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        int topNum = (num - 4) / 4;
        int id = index;
        for (int i = 1; i <= num; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 0, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = 3; i <= topNum + 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 3; i <= topNum * 2 + 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 2 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = 3; i <= topNum * 2 + 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 2 + 3 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 4; i <= topNum * 4 + 3; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum * 2 - 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 4; i <= topNum * 4 + 3; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 4 + 4 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        return workflowTaskRelationList;
    }

    private static List<WorkflowTaskRelation> getMontageTaskRelations(long projectCode, int num, int index,
                                                                      int offset) {
        List<WorkflowTaskRelation> workflowTaskRelationList = new ArrayList<>();
        int topNum = (num - 8) / 3;
        int id = index;
        for (int i = 1; i <= num; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, 0, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 1; i <= topNum * 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 2; i <= topNum * 2 + 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum - 2 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 2; i <= topNum * 2 + 1; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum - 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum + 1; i <= topNum * 2 + 2; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 2 + 3 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 4; i <= topNum * 2 + 4; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 5; i <= topNum * 3 + 4; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, topNum * 2 + 4 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 5; i <= topNum * 3 + 4; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - topNum * 2 - 4 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 2 + 5; i <= topNum * 3 + 4; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i + offset, 0, topNum * 3 + 5 + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        for (int i = topNum * 3 + 6; i <= topNum * 3 + 8; i++) {
            WorkflowTaskRelation workflowTaskRelation = new WorkflowTaskRelation("", 1,
                    projectCode, 0, i - 1 + offset, 0, i + offset, 0,
                    ConditionType.NONE, "{}");
            workflowTaskRelation.setId(id);
            workflowTaskRelationList.add(workflowTaskRelation);
            id++;
        }
        return workflowTaskRelationList;
    }

    private static WorkflowDefinition getSelfWorkflowDefinition(long projectCode, String name, int num) {
        StringJoiner jsonArray = new StringJoiner(",", "[", "]");
        for (int i = 1; i <= num; i++) {
            // 生成每个 JSON 对象
            String jsonObject = String.format("{\"taskCode\":%d,\"x\":0,\"y\":0}", i);
            jsonArray.add(jsonObject);
        }
        String locations = jsonArray.toString();
        WorkflowDefinition workflowDefinition =
                new WorkflowDefinition(projectCode, name, 0, "", "[]", locations, 0, 1);
        workflowDefinition.setId(1);
        workflowDefinition.setFlag(Flag.YES);
        workflowDefinition.setVersion(0);
        workflowDefinition.setReleaseState(ReleaseState.OFFLINE);
        workflowDefinition.setExecutionType(WorkflowExecutionTypeEnum.PARALLEL);
        return workflowDefinition;
    }

    private static List<TaskDefinition> getSelfTaskDefinitionList(long projectCode, int num) {
        List<TaskDefinition> taskDefinitionList = new ArrayList<>();
        for (int i = 1; i <= num; i++) {
            String name = "t" + i;
            String taskParams =
                    "{\"localParams\":[],\"resourceList\":[],\"type\":\"MYSQL\",\"datasource\":1,\"sql\":\"SELECT * FROM ssq_data\\r\\nWHERE id BETWEEN 0 AND 10\",\"sqlType\":\"0\",\"preStatements\":[],\"postStatements\":[],\"displayRows\":10}";
            TaskDefinition taskDefinition = new TaskDefinition(i, 0);
            taskDefinition.setId(i);
            taskDefinition.setName(name);
            taskDefinition.setProjectCode(projectCode);
            taskDefinition.setTaskType("SQL");
            taskDefinition.setTaskParams(taskParams);
            taskDefinition.setFlag(Flag.YES);
            taskDefinition.setIsCache(Flag.NO);
            taskDefinition.setTaskPriority(Priority.MEDIUM);
            taskDefinition.setWorkerGroup("default");
            taskDefinition.setEnvironmentCode(-1);
            taskDefinition.setTimeoutFlag(TimeoutFlag.CLOSE);
            Date now = new Date();
            taskDefinition.setCreateTime(now);
            taskDefinition.setUpdateTime(now);
            taskDefinitionList.add(taskDefinition);
        }
        return taskDefinitionList;
    }
}

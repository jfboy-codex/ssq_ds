package org.apache.dolphinscheduler.dao.entity;

import lombok.Data;

@Data
public class IntermediateData {

    private TaskInstance taskInstance;   // 数据的任务来源

    private String host;        // 数据的位置（即任务执行的位置）

    private long dataSize;      // 数据量bite

    public IntermediateData(TaskInstance taskInstance, String host, long dataSize) {
        this.taskInstance = taskInstance;
        this.host = host;
        this.dataSize = dataSize;
    }

}

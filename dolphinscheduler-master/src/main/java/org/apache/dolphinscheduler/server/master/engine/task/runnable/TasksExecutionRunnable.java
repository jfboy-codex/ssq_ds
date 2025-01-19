package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import lombok.Data;
import org.apache.dolphinscheduler.dao.entity.Bandwidth;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;
import org.apache.dolphinscheduler.dao.entity.IntermediateData;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.HostThread;

import java.util.List;
import java.util.Map;

@Data
public class TasksExecutionRunnable {

    private long dispatchCode; // 调度编号,也是开始调度时间戳

    private String dispatchType;  // 调度策略

    private String workerGroup;   // 工作组

    private Map<Host, Integer> hostThreadNum; // host线程数

    private BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心线程索引

    private List<ITaskExecutionRunnable> taskExecutionRunnableList; // 任务列表

    private BiMap<Integer, Long> taskBiMap; // 任务索引

    private BiMap<Integer, Long> dataBiMap; // 数据索引

    private List<Bandwidth> bandwidthList;  // 带宽列表

    private Table<Host, Host, Double> bandwidthTable; //a(j,j')

    private List<HostThreadStatus> hostThreadStatusList; // 线程状态列表

    private Table<Host, Integer, Long> hostUsableTimeTable; //w(host,threadId)

    private Table<Long, Long, IntermediateData> tasksNeedTasksData; //任务t1需要任务t2的中间数据信息

    private Map<Long, HostThread> tasksHostThreadMap; // 任务放置位置

    private Map<Long, HostThread> dataHostThreadMap; // 数据放置位置

}

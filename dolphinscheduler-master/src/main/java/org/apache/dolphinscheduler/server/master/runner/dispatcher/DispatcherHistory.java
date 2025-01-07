package org.apache.dolphinscheduler.server.master.runner.dispatcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.extract.base.utils.Host;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

@Data
@Builder
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DispatcherHistory {

    public static ThreadLocal<Map<TaskDefinition, Host>> threadLocal = new ThreadLocal<>();

    public static void setCurrentTaskHostMap(Map<TaskDefinition, Host> taskHostMap) {
        threadLocal.set(taskHostMap);
    }

    public static Map<TaskDefinition, Host> getCurrentTaskHostMap() {
        if(threadLocal.get()!=null) {
            return threadLocal.get();
        } else {
            return new HashMap<>();
        }
    }

    public static void removeCurrentTaskHostMap() {
        threadLocal.remove();
    }
}

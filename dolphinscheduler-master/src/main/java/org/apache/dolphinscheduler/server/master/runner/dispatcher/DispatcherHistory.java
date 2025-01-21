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

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.extract.base.utils.Host;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

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
        if (threadLocal.get() != null) {
            return threadLocal.get();
        } else {
            return new HashMap<>();
        }
    }

    public static void removeCurrentTaskHostMap() {
        threadLocal.remove();
    }
}

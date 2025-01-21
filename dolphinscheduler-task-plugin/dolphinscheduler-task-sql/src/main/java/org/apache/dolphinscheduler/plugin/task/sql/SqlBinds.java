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

package org.apache.dolphinscheduler.plugin.task.sql;

import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to contains both prepared sql string and its to-be-bind parameters
 */
public class SqlBinds {

    private final String sql;
    private final Map<Integer, Property> paramsMap;

    public SqlBinds(String sql, Map<Integer, Property> paramsMap) {
        this.sql = sql;
        this.paramsMap = paramsMap;
    }

    public String getSql() {
        return sql;
    }

    public Map<Integer, Property> getParamsMap() {
        return paramsMap;
    }

    public static SqlBinds getMiniSqlBinds(SqlBinds sqlBinds) {
        int miniSize = 200;
        String miniSql = sqlBinds.getSql();
        if (sqlBinds.getSql() != null && sqlBinds.getSql().length() > miniSize) {
            String sql = sqlBinds.getSql();
            miniSql = sql.substring(0, miniSize) + "......" + sql.substring(sql.length() - 3);
        }
        Map<Integer, Property> miniParamsMap = new HashMap<>();
        int num = 0;
        for (Map.Entry<Integer, Property> entry : sqlBinds.getParamsMap().entrySet()) {
            Integer key = entry.getKey();
            Property property = entry.getValue();
            if (num < 10) {
                miniParamsMap.put(key, property);
                num++;
            } else {
                break;
            }
        }
        SqlBinds miniSqlBinds = new SqlBinds(miniSql, miniParamsMap);
        return miniSqlBinds;
    }
}

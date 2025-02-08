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

package org.apache.dolphinscheduler.server.master.cluster.solver;

import org.apache.dolphinscheduler.extract.base.utils.HostThread;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;

@Slf4j
public class YSolverGreedy {

    private final int M; // 数据数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> dataBiMap; // 任务索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    // 成本矩阵
    private final long[][] costMatrix;

    @Getter
    long optimalTime = 0; // 最优完成时间

    Map<Long, HostThread> dataHostThreadMap = new HashMap<>(); // 存储数据放置位置的结果

    public YSolverGreedy(BiMap<Integer, Long> dataBiMap, BiMap<Integer, HostThread> hostThreadBiMap,
                         Table<Long, HostThread, Long> timeTable) {
        M = dataBiMap.size();
        J = hostThreadBiMap.size();
        this.dataBiMap = dataBiMap;
        this.hostThreadBiMap = hostThreadBiMap;
        this.timeTable = timeTable;
        this.costMatrix = new long[M][J];
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < J; j++) {
                long dataCode = dataBiMap.get(i);
                HostThread hostThread = hostThreadBiMap.get(j);
                costMatrix[i][j] = timeTable.get(dataCode, hostThread);
            }
        }
    }

    public Map<Long, HostThread> solve() {
        for (int i = 0; i < M; i++) {
            long minVal = costMatrix[i][0];
            int minIndex = 0;

            for (int j = 1; j < J; j++) {
                if (costMatrix[i][j] < minVal) {
                    minVal = costMatrix[i][j];
                    minIndex = j;
                }
            }
            if (minVal > optimalTime) {
                optimalTime = minVal;
            }
            long dataCode = dataBiMap.get(i);
            HostThread hostThread = hostThreadBiMap.get(minIndex);
            dataHostThreadMap.put(dataCode, hostThread);
        }
        return dataHostThreadMap;
    }

    // 打印矩阵
    public static void printMatrix(long[][] matrix) {
        for (long[] row : matrix) {
            for (long value : row) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
        System.out.println();
    }
}

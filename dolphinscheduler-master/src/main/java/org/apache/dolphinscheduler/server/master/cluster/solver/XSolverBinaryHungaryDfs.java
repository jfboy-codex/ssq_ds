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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;

@Slf4j
public class XSolverBinaryHungaryDfs {

    private final int T; // 任务数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> taskBiMap; // 任务索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    // 成本矩阵
    private final long[][] costMatrix;

    // 二分图的邻接矩阵，graph[i][j] = 1 表示左边第 i 个顶点和右边第 j 个顶点之间有边相连
    private final int[][] graph;

    // 用于标记右边顶点集合中的顶点在一次深度优先搜索过程中是否已经被访问过
    private final boolean[] visited;

    // 用于记录右边顶点集合中的每个顶点与左边顶点集合中的哪个顶点匹配，初始值 -1 表示未匹配
    private final int[] match;

    @Getter
    long optimalTime = 0; // 最优完成时间

    Map<Long, HostThread> taskHostThreadMap = new HashMap<>(); // 存储任务放置位置的结果

    public XSolverBinaryHungaryDfs(BiMap<Integer, Long> taskBiMap, BiMap<Integer, HostThread> hostThreadBiMap,
                                   Table<Long, HostThread, Long> timeTable) {
        T = taskBiMap.size();
        J = hostThreadBiMap.size();
        this.taskBiMap = taskBiMap;
        this.hostThreadBiMap = hostThreadBiMap;
        this.timeTable = timeTable;
        this.costMatrix = new long[T][J];
        for (int i = 0; i < T; i++) {
            for (int j = 0; j < J; j++) {
                long taskCode = taskBiMap.get(i);
                HostThread hostThread = hostThreadBiMap.get(j);
                costMatrix[i][j] = timeTable.get(taskCode, hostThread);
            }
        }
        this.graph = new int[T][J];
        // 初始化 visited 数组，长度为右边顶点的数量，默认所有顶点都未被访问
        this.visited = new boolean[J];
        // 初始化 match 数组，长度为右边顶点的数量，所有元素初始值为 -1，表示初始时没有匹配关系
        this.match = new int[J];
        Arrays.fill(match, -1);
    }

    public Map<Long, HostThread> solve() {
        if (T > J) {
            log.error("单次调度任务数大于数据中心可执行线程总数!!!");
            // return new HashMap<>();
        }
        // printMatrix(costMatrix);
        TreeSet<Long> timeSet = new TreeSet<>(); // 升序不重复
        for (int i = 0; i < T; i++) {
            for (int j = 0; j < J; j++) {
                timeSet.add(costMatrix[i][j]);
            }
        }
        List<Long> timeList = new ArrayList<>(timeSet);
        int left = 0;
        int right = timeList.size() - 1;
        int[] matchResult = new int[J];
        Arrays.fill(matchResult, -1);
        while (left <= right) {
            Arrays.fill(match, -1);
            int mid = left + (right - left) / 2;
            long k = timeList.get(mid);
            for (int i = 0; i < T; i++) {
                for (int j = 0; j < J; j++) {
                    if (costMatrix[i][j] <= k) {
                        graph[i][j] = 1;
                    }
                }
            }
            // 调用 hungary 方法计算二分图的最大匹配数
            int maxMatching = hungray();
            // 如果最大匹配数等于左边和右边顶点集合中较小的顶点数量，说明存在完全匹配
            if (maxMatching == Math.min(T, J)) {
                right = mid - 1;
                matchResult = Arrays.copyOf(match, match.length);
            } else {
                left = mid + 1;
            }
        }
        for (int j = 0; j < J; j++) {
            if (matchResult[j] != -1) {
                long taskCode = taskBiMap.get(matchResult[j]);
                HostThread hostThread = hostThreadBiMap.get(j);
                long time = timeTable.get(taskCode, hostThread);
                if (time > optimalTime) {
                    optimalTime = time;
                }
                taskHostThreadMap.put(taskCode, hostThread);
            }
        }
        return taskHostThreadMap;
    }

    private int hungray() {
        // 用于记录最大匹配的边数，初始值为 0
        int result = 0;
        // 遍历左边顶点集合中的所有顶点
        for (int u = 0; u < T; u++) {
            // 每次对一个新的左边顶点进行搜索前，将 visited 数组重置，确保每个顶点在本次搜索中未被标记
            Arrays.fill(visited, false);
            // 调用 dfs 方法从左边顶点 u 出发寻找增广路径，如果找到了则最大匹配边数加 1
            if (dfs(u)) {
                result++;
            }
        }
        // 返回最大匹配的边数
        return result;
    }

    private boolean dfs(int u) {
        // 遍历右边顶点集合中的所有顶点
        for (int v = 0; v < J; v++) {
            // 如果左边顶点 u 和右边顶点 v 之间有边相连，并且右边顶点 v 在本次搜索中还未被访问过
            if (graph[u][v] == 1 && !visited[v]) {
                // 标记右边顶点 v 为已访问
                visited[v] = true;
                // 如果右边顶点 v 还没有匹配的左边顶点，或者右边顶点 v 当前匹配的左边顶点可以找到新的匹配
                if (match[v] == -1 || dfs(match[v])) {
                    // 更新右边顶点 v 的匹配对象为左边顶点 u
                    match[v] = u;
                    // 表示找到了一条增广路径，返回 true
                    return true;
                }
            }
        }
        // 从左边顶点 u 出发没有找到增广路径，返回 false
        return false;
    }

    // 打印矩阵
    public static void printMatrix(long[][] matrix) {
        System.out.println("Matrix:");
        for (long[] row : matrix) {
            for (long value : row) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
        System.out.println();
    }
}

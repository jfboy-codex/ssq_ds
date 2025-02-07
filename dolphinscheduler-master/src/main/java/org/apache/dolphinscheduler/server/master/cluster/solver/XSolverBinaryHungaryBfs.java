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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;

@Slf4j
public class XSolverBinaryHungaryBfs {

    private final int T; // 任务数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> taskBiMap; // 任务索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    // 成本矩阵
    private long[][] costMatrix;

    // 二分图的邻接矩阵，graph[i][j] = 1 表示左边第 i 个顶点和右边第 j 个顶点之间有边相连
    private int[][] graph;

    // 记录左边顶点的匹配情况，matchLeft[i] 表示左边第 i 个顶点匹配的右边顶点编号，-1 表示未匹配
    private int[] matchLeft;

    // 记录右边顶点的匹配情况，matchRight[j] 表示右边第 j 个顶点匹配的左边顶点编号，-1 表示未匹配
    private int[] matchRight;

    // 记录路径上每个顶点的前驱，用于回溯增广路径
    private int[] pre;

    @Getter
    long optimalTime = 0; // 最优完成时间

    Map<Long, HostThread> taskHostThreadMap = new HashMap<>(); // 存储任务放置位置的结果

    public XSolverBinaryHungaryBfs(BiMap<Integer, Long> taskBiMap, BiMap<Integer, HostThread> hostThreadBiMap,
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
        // 初始化 matchLeft 数组，长度为左边顶点的数量，所有元素初始值为 -1，表示初始时没有匹配关系
        this.matchLeft = new int[T];
        // 初始化 matchRight 数组，长度为右边顶点的数量，所有元素初始值为 -1，表示初始时没有匹配关系
        this.matchRight = new int[J];
        // 初始化 pre 数组，长度为左右顶点总数，所有元素初始值为 -1，表示初始时没有前驱信息
        this.pre = new int[T + J];
        Arrays.fill(matchLeft, -1);
        Arrays.fill(matchRight, -1);
        Arrays.fill(pre, -1);
    }

    public Map<Long, HostThread> solve() {
        if (T > J) {
            log.error("单次调度任务数大于数据中心可执行线程总数!!!");
            // return new HashMap<>();
        }
        TreeSet<Long> timeSet = new TreeSet<>(); // 升序不重复
        for (int i = 0; i < T; i++) {
            for (int j = 0; j < J; j++) {
                timeSet.add(costMatrix[i][j]);
            }
        }
        List<Long> timeList = new ArrayList<>(timeSet);
        int left = 0;
        int right = timeList.size() - 1;
        int[] matchLeftResult = new int[J];
        Arrays.fill(matchLeftResult, -1);
        while (left <= right) {
            Arrays.fill(matchLeft, -1);
            Arrays.fill(matchRight, -1);
            Arrays.fill(pre, -1);
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
            // System.out.println(left + " " + right + " " + maxMatching);
            // 如果最大匹配数等于左边和右边顶点集合中较小的顶点数量，说明存在完全匹配
            if (maxMatching == Math.min(T, J)) {
                right = mid - 1;
                matchLeftResult = Arrays.copyOf(matchLeft, matchLeft.length);
            } else {
                left = mid + 1;
            }
        }
        for (int i = 0; i < T; i++) {
            if (matchLeftResult[i] != -1) {
                long taskCode = taskBiMap.get(i);
                HostThread hostThread = hostThreadBiMap.get(matchLeftResult[i]);
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
        for (int i = 0; i < T; i++) {
            // 如果左边顶点 i 还没有匹配
            if (matchLeft[i] == -1) {
                // 调用 bfs 方法从左边顶点 i 出发寻找增广路径，如果找到了则最大匹配边数加 1
                if (bfs(i)) {
                    result++;
                }
            }
        }
        // 返回最大匹配的边数
        return result;
    }

    // 使用广度优先搜索（BFS）寻找增广路径
    private boolean bfs(int start) {
        // 标记数组，用于记录顶点是否被访问过，visited[i] 表示第 i 个顶点的访问状态
        boolean[] visited = new boolean[T + J];
        // 队列，用于 BFS 搜索，存储待访问的顶点
        Queue<Integer> queue = new LinkedList<>();
        // 将起始的左边顶点加入队列
        queue.offer(start);
        // 标记起始顶点为已访问
        visited[start] = true;

        // 当队列不为空时，继续进行 BFS 搜索
        while (!queue.isEmpty()) {
            // 从队列中取出一个顶点
            int u = queue.poll();
            // 如果当前顶点是左边的顶点
            if (u < T) {
                // 遍历右边顶点集合中的所有顶点
                for (int v = 0; v < J; v++) {
                    // 如果左边顶点 u 和右边顶点 v 之间有边相连，并且右边顶点 v 在本次搜索中还未被访问过
                    if (graph[u][v] == 1 && !visited[T + v]) {
                        // 标记右边顶点 v 为已访问
                        visited[T + v] = true;
                        // 记录右边顶点 v 的前驱为左边顶点 u
                        pre[T + v] = u;
                        // 如果右边顶点 v 还没有匹配的左边顶点，说明找到了一条增广路径
                        if (matchRight[v] == -1) {
                            // 从右边顶点 v 开始回溯增广路径
                            int t = T + v;
                            // 不断回溯，更新匹配信息
                            while (t != -1) {
                                // 如果当前顶点是左边顶点
                                if (t < T) {
                                    // 更新左边顶点的匹配信息
                                    matchLeft[t] = (t - T) * (-1);
                                } else {
                                    // 如果当前顶点是右边顶点，更新右边顶点的匹配信息
                                    matchRight[t - T] = pre[t];
                                }
                                // 移动到前驱顶点
                                t = pre[t];
                            }
                            // 找到增广路径，返回 true
                            return true;
                        } else {
                            // 如果右边顶点 v 已经匹配，将其匹配的左边顶点加入队列继续搜索
                            queue.offer(matchRight[v]);
                            // 标记该左边顶点为已访问
                            visited[matchRight[v]] = true;
                            // 记录该左边顶点的前驱为右边顶点 v
                            pre[matchRight[v]] = T + v;
                        }
                    }
                }
            }
        }
        // 没有找到增广路径，返回 false
        return false;
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

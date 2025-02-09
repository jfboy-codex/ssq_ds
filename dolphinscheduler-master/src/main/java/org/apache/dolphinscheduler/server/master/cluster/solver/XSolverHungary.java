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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;

@Slf4j
public class XSolverHungary {

    private final int T; // 任务数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> taskBiMap; // 任务索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    @Getter
    long optimalTime = 0; // 最优完成时间

    Map<Long, HostThread> taskHostThreadMap = new HashMap<>(); // 存储任务放置位置的结果

    public XSolverHungary(BiMap<Integer, Long> taskBiMap, BiMap<Integer, HostThread> hostThreadBiMap,
                          Table<Long, HostThread, Long> timeTable) {
        T = taskBiMap.size();
        J = hostThreadBiMap.size();
        this.taskBiMap = taskBiMap;
        this.hostThreadBiMap = hostThreadBiMap;
        this.timeTable = timeTable;
    }

    public Map<Long, HostThread> solve() {
        if (T > J) {
            log.error("单次调度任务数大于数据中心可执行线程总数!!!");
            return new HashMap<>();
        }
        long[][] costMatrix = new long[J][J];
        for (int i = 0; i < T; i++) {
            for (int j = 0; j < J; j++) {
                long taskCode = taskBiMap.get(i);
                HostThread hostThread = hostThreadBiMap.get(j);
                costMatrix[i][j] = timeTable.get(taskCode, hostThread);
            }
        }
        int[][] coverMatrix = new int[J][J]; // 全0矩阵
        List<int[]> markedZeros = new ArrayList<>();
        printMatrix(costMatrix);
        reduceRows(costMatrix);
        reduceColumns(costMatrix);
        printMatrix(costMatrix);
        boolean flag = coverZeros(costMatrix, coverMatrix, markedZeros);
        while (!flag) {
            adjustMatrix(costMatrix, markedZeros);
            flag = coverZeros(costMatrix, coverMatrix, markedZeros);
        }
        for (int[] markedZero : markedZeros) {
            if (markedZero[0] < T) {
                long taskCode = taskBiMap.get(markedZero[0]);
                HostThread hostThread = hostThreadBiMap.get(markedZero[1]);
                long time = timeTable.get(taskCode, hostThread);
                if (time > optimalTime) {
                    optimalTime = time;
                }
                taskHostThreadMap.put(taskCode, hostThread);
            }
        }
        return taskHostThreadMap;
    }

    // 对每一行减去该行的最小值
    public static void reduceRows(long[][] matrix) {
        for (int i = 0; i < matrix.length; i++) {
            // 找到当前行的最小值
            long min = matrix[i][0];
            for (int j = 1; j < matrix[i].length; j++) {
                if (matrix[i][j] < min) {
                    min = matrix[i][j];
                }
            }
            // 对当前行的每个元素减去最小值
            for (int j = 0; j < matrix[i].length; j++) {
                matrix[i][j] -= min;
            }
        }
    }

    // 对每一列减去该列的最小值
    public static void reduceColumns(long[][] matrix) {
        for (int j = 0; j < matrix[0].length; j++) {
            // 找到当前列的最小值
            long min = matrix[0][j];
            for (int i = 1; i < matrix.length; i++) {
                if (matrix[i][j] < min) {
                    min = matrix[i][j];
                }
            }
            // 对当前列的每个元素减去最小值
            for (int i = 0; i < matrix.length; i++) {
                matrix[i][j] -= min;
            }
        }
    }

    public static boolean coverZeros(long[][] costMatrix, int[][] coverMatrix, List<int[]> markedZeros) {
        while (!isCoveredAllZero(costMatrix, coverMatrix)) {
            coverRowZero(costMatrix, coverMatrix, markedZeros);
        }
        return markedZeros.size() == costMatrix.length;
    }

    public static void coverRowZero(long[][] costMatrix, int[][] coverMatrix, List<int[]> markedZeros) {
        int coverRow = -1;
        int zeroColumn = -1;
        int minRowZeroNum = costMatrix.length + 1;
        for (int i = 0; i < costMatrix.length; i++) {
            int zeroNum = 0;
            int firstZeroIndex = -1;
            for (int j = 0; j < costMatrix[i].length; j++) {
                // 未被划掉的0的数量
                if (costMatrix[i][j] == 0 && coverMatrix[i][j] == 0) {
                    if (firstZeroIndex == -1) {
                        firstZeroIndex = j;
                    }
                    zeroNum++;
                }
            }
            if (zeroNum != 0 && zeroNum < minRowZeroNum) {
                minRowZeroNum = zeroNum;
                coverRow = i;
                zeroColumn = firstZeroIndex;
            }
        }
        markedZeros.add(new int[]{coverRow, zeroColumn});
        for (int i = 0; i < coverMatrix.length; i++) {
            for (int j = 0; j < coverMatrix[i].length; j++) {
                if (i == coverRow || j == zeroColumn) {
                    coverMatrix[i][j] = 1;
                }
            }
        }
    }

    public static void adjustMatrix(long[][] costMatrix, List<int[]> markedZeros) {
        List<Integer> tickedRow = new ArrayList<>();
        List<Integer> tickedColumn = new ArrayList<>();
        tickLine(costMatrix, markedZeros, tickedRow, tickedColumn);
        List<Integer> linedRow = new ArrayList<>();
        List<Integer> linedColumn = new ArrayList<>(tickedColumn);
        for (int i = 0; i < costMatrix.length; i++) {
            if (!tickedRow.contains(i)) {
                linedRow.add(i);
            }
        }
        List<Long> noLinedElements = new ArrayList<>();
        for (int i = 0; i < costMatrix.length; i++) {
            if (!linedRow.contains(i)) {
                for (int j = 0; j < costMatrix[i].length; j++) {
                    if (!linedColumn.contains(j)) {
                        noLinedElements.add(costMatrix[i][j]);
                    }
                }
            }
        }
        if (noLinedElements.isEmpty()) {
            log.error("所有数都被划过！");
        }
        long miniK = Collections.min(noLinedElements);
        for (int i = 0; i < costMatrix.length; i++) {
            for (int j = 0; j < costMatrix[i].length; j++) {
                if (linedRow.contains(i) && linedColumn.contains(j)) {
                    costMatrix[i][j] += miniK;
                } else if (!linedRow.contains(i) && !linedColumn.contains(j)) {
                    costMatrix[i][j] -= miniK;
                }
            }
        }
    }

    public static void tickLine(long[][] costMatrix, List<int[]> markedZeros, List<Integer> tickedRow,
                                List<Integer> tickedColumn) {
        boolean tickEd = true;
        System.out.println("************");
        printMatrix(costMatrix);
        System.out.println("独立零元素");
        for (int[] markedZero : markedZeros) {
            System.out.print(markedZero[0] + "," + markedZero[1] + "   ");
        }
        System.out.println();
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < costMatrix.length; i++) {
            list.add(i);
        }
        for (int[] mark : markedZeros) {
            list.remove(Integer.valueOf(mark[0]));
        }
        for (Integer i : list) {
            tickedRow.add(i);
            System.out.println("打勾行增加：" + i);
        }
        while (tickEd) {
            tickEd = false;
            for (int row : tickedRow) {
                for (int col = 0; col < costMatrix[row].length; col++) {
                    if (costMatrix[row][col] == 0) {
                        if (!tickedColumn.contains(col)) {
                            tickedColumn.add(col);
                            System.out.println("打勾列增加：" + col);
                            tickEd = true;
                        }
                    }
                }
            }
            for (int col : tickedColumn) {
                for (int row = 0; row < costMatrix.length; row++) {
                    if (costMatrix[row][col] == 0) {
                        for (int[] mark : markedZeros) {
                            if (mark[0] == row && mark[1] == col) {
                                if (!tickedRow.contains(row)) {
                                    tickedRow.add(row);
                                    System.out.println("打勾行增加：" + row);
                                    tickEd = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("最终打勾行为：");
        for (Integer row : tickedRow) {
            System.out.print(row + " ");
        }
        System.out.println();
        System.out.println("最终打勾列为：");
        for (Integer col : tickedColumn) {
            System.out.print(col + " ");
        }
        System.out.println();
        System.out.println("************");
    }

    public static boolean isCoveredAllZero(long[][] costMatrix, int[][] coverMatrix) {
        for (int i = 0; i < costMatrix.length; i++) {
            for (int j = 0; j < costMatrix[i].length; j++) {
                if (costMatrix[i][j] == 0 && coverMatrix[i][j] == 0) {
                    return false;
                }
            }
        }
        return true;
    }

    // 打印矩阵
    public static void printMatrix(long[][] matrix) {
        System.out.println("成本矩阵：");
        for (long[] row : matrix) {
            for (long value : row) {
                System.out.print(value + " ");
            }
            System.out.println();
        }
        System.out.println();
    }
}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;
import com.gurobi.gurobi.GRB;
import com.gurobi.gurobi.GRBEnv;
import com.gurobi.gurobi.GRBException;
import com.gurobi.gurobi.GRBLinExpr;
import com.gurobi.gurobi.GRBModel;
import com.gurobi.gurobi.GRBVar;

@Slf4j
public class YSolverGurobi {

    private final int M; // 任务数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> dataBiMap; // 数据索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    @Getter
    long optimalTime; // 最优完成时间

    Map<Long, HostThread> dataHostThreadMap = new HashMap<>(); // 存储数据放置位置的结果

    public YSolverGurobi(BiMap<Integer, Long> dataBiMap, BiMap<Integer, HostThread> hostThreadBiMap,
                         Table<Long, HostThread, Long> timeTable) {
        M = dataBiMap.size();
        J = hostThreadBiMap.size();
        this.dataBiMap = dataBiMap;
        this.hostThreadBiMap = hostThreadBiMap;
        this.timeTable = timeTable;
    }

    public Map<Long, HostThread> solve() {
        try {
            GRBEnv env = new GRBEnv();
            env.set(GRB.IntParam.OutputFlag, 0);
            GRBModel model = new GRBModel(env);

            List<GRBVar> xijVars = new ArrayList<>();
            GRBVar k = model.addVar(0, GRB.INFINITY, 0, GRB.CONTINUOUS, "k");
            for (int t = 0; t < M; t++) {
                for (int j = 0; j < J; j++) {
                    // 定义xi,j变量，设置为二进制变量（0或1）
                    GRBVar xij = model.addVar(0, 1, 0, GRB.BINARY, "x(" + t + "," + j + ")");
                    xijVars.add(xij);
                }
            }

            // 设置目标函数（目标函数为k）
            GRBLinExpr objective = new GRBLinExpr();
            objective.addTerm(1, k);
            model.setObjective(objective, GRB.MINIMIZE);

            // 约束条件：对于任意的i,j范围内yi,j*si,j<=k
            for (int m = 0; m < M; m++) {
                for (int j = 0; j < J; j++) {
                    GRBLinExpr constraintExpr = new GRBLinExpr();
                    // 假设si,j的值，示例中设为(i + j + 1)，实际需根据具体情况确定
                    double si_j = timeTable.get(dataBiMap.get(m), hostThreadBiMap.get(j));
                    constraintExpr.addTerm(si_j, xijVars.get(m * J + j));
                    constraintExpr.addTerm(-1, k);
                    model.addConstr(constraintExpr, GRB.LESS_EQUAL, 0, "constraint_1_" + m + "_" + j);
                }
            }

            // 约束条件：对于任意的i属于0-I，所有遍历0-J的yi,j之和等于1
            for (int m = 0; m < M; m++) {
                GRBLinExpr sumConstraintExpr = new GRBLinExpr();
                for (int j = 0; j < J; j++) {
                    sumConstraintExpr.addTerm(1, xijVars.get(m * J + j));
                }
                model.addConstr(sumConstraintExpr, GRB.EQUAL, 1, "constraint_2_" + m);
            }

            // 求解模型
            model.optimize();

            // 输出结果
            if (model.get(GRB.IntAttr.Status) == GRB.Status.OPTIMAL) {
                // log.info("最优解(任务最短完成时间): {}", k.get(GRB.DoubleAttr.X));
                for (int m = 0; m < M; m++) {
                    for (int j = 0; j < J; j++) {
                        int x = (int) xijVars.get(m * J + j).get(GRB.DoubleAttr.X);
                        if (x == 1) {
                            dataHostThreadMap.put(dataBiMap.get(m), hostThreadBiMap.get(j));
                            break;
                        }
                    }
                }
                optimalTime = (long) model.get(GRB.DoubleAttr.ObjVal);
                // log.info("目标函数最优值: {}", model.get(GRB.DoubleAttr.ObjVal));
            } else {
                log.info("问题无解或未达到最优解...");
            }

            // 释放资源
            model.dispose();
            env.dispose();

            // 返回结果
            return dataHostThreadMap;

        } catch (GRBException e) {
            log.info("Error code: {} . {}", e.getErrorCode(), e.getMessage());
            return dataHostThreadMap;
        }
    }
}

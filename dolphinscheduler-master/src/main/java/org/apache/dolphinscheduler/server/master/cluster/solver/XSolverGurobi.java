package org.apache.dolphinscheduler.server.master.cluster.solver;

import com.google.common.collect.HashBiMap;
import gurobi.*;
import com.google.common.collect.BiMap;
import com.google.common.collect.Table;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.base.utils.HostThread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class XSolverGurobi {

    private final int T; // 任务数量

    private final int J; // 数据中心数量

    BiMap<Integer, Long> taskBiMap; // 任务索引

    BiMap<Integer, HostThread> hostThreadBiMap; // 数据中心索引

    Table<Long, HostThread, Long> timeTable; // 时间矩阵

    @Getter
    long optimalTime; // 最优完成时间

    Map<Long, HostThread> taskHostThreadBiMap = new HashMap<>(); // 存储任务放置位置的结果


    public XSolverGurobi(BiMap<Integer, Long> taskBiMap, BiMap<Integer, HostThread> hostThreadBiMap, Table<Long, HostThread, Long> timeTable) {
        T = taskBiMap.size();
        J = hostThreadBiMap.size();
        this.taskBiMap = taskBiMap;
        this.hostThreadBiMap = hostThreadBiMap;
        this.timeTable = timeTable;
    }

    public Map<Long, HostThread> solve() {
        try {
            GRBEnv    env   = new GRBEnv();
            env.set(GRB.IntParam.OutputFlag, 0);
            GRBModel  model = new GRBModel(env);

            List<GRBVar> xijVars = new ArrayList<>();
            GRBVar k = model.addVar(0, GRB.INFINITY, 0, GRB.CONTINUOUS, "k");
            for (int t = 0; t < T; t++) {
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

            // 约束条件：对于任意的i,j范围内xi,j*si,j<=k
            for (int t = 0; t < T; t++) {
                for (int j = 0; j < J; j++) {
                    GRBLinExpr constraintExpr = new GRBLinExpr();
                    // 假设si,j的值，示例中设为(i + j + 1)，实际需根据具体情况确定
                    double si_j = timeTable.get(taskBiMap.get(t), hostThreadBiMap.get(j));
                    constraintExpr.addTerm(si_j, xijVars.get(t * J + j));
                    constraintExpr.addTerm(-1, k);
                    model.addConstr(constraintExpr, GRB.LESS_EQUAL, 0, "constraint_1_" + t + "_" + j);
                }
            }

            // 约束条件：对于任意的i属于0-I，所有遍历0-J的xi,j之和等于1
            for (int t = 0; t < T; t++) {
                GRBLinExpr sumConstraintExpr = new GRBLinExpr();
                for (int j = 0; j < J; j++) {
                    sumConstraintExpr.addTerm(1, xijVars.get(t * J + j));
                }
                model.addConstr(sumConstraintExpr, GRB.EQUAL, 1, "constraint_2_" + t);
            }

            // 约束条件：对于任意的j属于0-J，所有遍历0-I的xi,j之和小于等于1
            for (int j=0; j < J; j++) {
                GRBLinExpr sumConstraintExpr = new GRBLinExpr();
                for (int t=0; t < T; t++) {
                    sumConstraintExpr.addTerm(1, xijVars.get(t * J + j));
                }
                model.addConstr(sumConstraintExpr, GRB.LESS_EQUAL, 1, "constraint_3_" + j);
            }

            // 求解模型
            model.optimize();

            // 输出结果
            if (model.get(GRB.IntAttr.Status) == GRB.Status.OPTIMAL) {
                // log.info("最优解(任务最短完成时间): {}", k.get(GRB.DoubleAttr.X));
                for (int t = 0; t < T; t++) {
                    for (int j = 0; j < J; j++) {
                        int x = (int) xijVars.get(t * J + j).get(GRB.DoubleAttr.X);
                        if (x==1) {
                            taskHostThreadBiMap.put(taskBiMap.get(t), hostThreadBiMap.get(j));
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
            return taskHostThreadBiMap;

        } catch (GRBException e) {
            log.info("Error code: {} . {}",  e.getErrorCode(), e.getMessage());
            return taskHostThreadBiMap;
        }
    }
}

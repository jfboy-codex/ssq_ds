package org.apache.dolphinscheduler.server.master.cluster.solver;

import com.google.common.collect.BiMap;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.NonNegativeConstraint;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.dolphinscheduler.extract.base.utils.Host;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class XSolverMath {

    private final int T; // 任务数量

    private final int J; // 数据中心数量

    Table<Long, Host, Long> timeTable; // 时间矩阵

    private final double[] objectiveCoefficients; // 目标函数系数
    private final List<LinearConstraint> constraints = new ArrayList<>(); // 约束条件

    public XSolverMath(BiMap<Integer, Long> taskBiMap, BiMap<Integer, Host> hostBiMap, Table<Long, Host, Long> timeTable) {
        T = taskBiMap.size();
        J = hostBiMap.size();
        this.timeTable = timeTable;
        int paramsNum = T*J + 1; // 参数数量，s和x(t,j)

        // 定义目标函数的系数
        objectiveCoefficients = new double[paramsNum];
        // s的系数设为1，x(t,j)的系数设为0（因为目标函数只是k）
        objectiveCoefficients[paramsNum-1] = 1;

        // 约束条件：x(t,j) * s(t,j) - k <= 0
        for (int t = 0; t < T; t++) {
            for (int j = 0; j < J; j++) {
                double[] constraintCoefficients = new double[paramsNum];
                // x(t,j)的系数设为s(t,j),k的系数设为-1
                constraintCoefficients[t * J + j] = timeTable.get(taskBiMap.get(t), hostBiMap.get(j));
                constraintCoefficients[paramsNum - 1] = -1;
                double rightSideValue = 0;
                constraints.add(new LinearConstraint(constraintCoefficients, Relationship.LEQ, rightSideValue));
            }
        }

        // 约束条件：对于任意的t属于0 - T，所有遍历0 - J的x(i,j)之和等于1
        for (int t = 0; t < T; t++) {
            double[] sumConstraintCoefficients = new double[paramsNum];
            for (int j = 0; j < J; j++) {
                sumConstraintCoefficients[t * T + j] = 1;
            }
            constraints.add(new LinearConstraint(sumConstraintCoefficients, Relationship.EQ, 1));
        }
    }

    public void solve() {
        // 创建线性目标函数
        LinearObjectiveFunction objectiveFunction = new LinearObjectiveFunction(objectiveCoefficients, 0);

        // 创建约束条件集合
        LinearConstraintSet constraintSet = new LinearConstraintSet(constraints);

        // 使用单纯形法求解
        SimplexSolver solver = new SimplexSolver();
        PointValuePair solution = solver.optimize(objectiveFunction, constraintSet, new NonNegativeConstraint(true), new MaxIter(1000));

        // 输出结果
        double[] point = solution.getPoint();
        log.info("task completion time = {}", point[point.length-1]);
        for (int i = 0; i < point.length-1; i++) {
            log.info("x({},{})={}", i/J, i%J, point[i]);
        }
    }
}

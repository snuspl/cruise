/*
* Copyright (C) 2017 Seoul National University
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package edu.snu.cay.dolphin.async.optimizer.impl.ilp;

import gurobi.*;

/**
 * Computes Dolphin's optimal cost and configuration (w.r.t. w, s, d, m).
 */
final class ILPSolver {
  ConfDescriptor optimize(final int n, final int dTotal, final int mTotal,
                final int p, final double[] cWProc, final double[] bandwidth) throws GRBException {
    final GRBEnv env = new GRBEnv("dolphin_solver.log");
    final GRBModel model = new GRBModel(env);

    ////////////////////////////////////////////////////////////////////////
    /////     Constants to bound the cost for linear approximation     /////
    ////////////////////////////////////////////////////////////////////////
    final double maxCompCost = dTotal * findMax(cWProc); // What if the slowest worker processes all data
    final double maxCommCost =
        p * mTotal * dTotal / findMin(bandwidth); // if the slowest worker/server becomes bottleneck.
    final double maxTotalCost = maxCompCost + maxCommCost;
    System.out.println("Max compCost: " + maxCompCost + " Max commCost: " + maxCommCost);


    /////////////////////////////////
    //////      Variables      //////
    /////////////////////////////////
    final GRBVar[] d = new GRBVar[n];
    final GRBVar[] m = new GRBVar[n];
    final GRBVar[] w = new GRBVar[n];
    final GRBVar[] s = new GRBVar[n];
    final GRBVar[] compCost = new GRBVar[n];
    final GRBVar[] commCost = new GRBVar[n];
    final GRBVar[] costI = new GRBVar[n];
    for (int i = 0; i < n; i++) {
      d[i] = model.addVar(0.0, dTotal, 0.0, GRB.INTEGER, String.format("d[%d]", i)); // C2
      m[i] = model.addVar(0.0, mTotal, 0.0, GRB.INTEGER, String.format("m[%d]", i)); // C6
      w[i] = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("w[%d]", i)); // C1
      s[i] = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("s[%d]", i)); // C5

      compCost[i] = model.addVar(0.0, maxCompCost, 0.0, GRB.CONTINUOUS, String.format("compCost[%d]", i));
      commCost[i] = model.addVar(0.0, maxCommCost, 0.0, GRB.CONTINUOUS, String.format("commCost[%d]", i));
      costI[i] = model.addVar(0.0, maxTotalCost, 0.0, GRB.CONTINUOUS, String.format("cost[%d]", i));
    }


    /////////////////////////////////////////////
    /////      Constrains on variables      /////
    /////////////////////////////////////////////
    final GRBVar numWorkers = model.addVar(1.0, n-1, 0.0, GRB.INTEGER,"W");
    final GRBLinExpr numWorkersExpr = sum(w);
    model.addConstr(numWorkers, GRB.EQUAL, numWorkersExpr, "nWorkers=sum(w[i])");

    final GRBVar numServers = model.addVar(1.0, n-1, 0.0, GRB.INTEGER,"S");
    final GRBLinExpr numServersExpr = sum(s);
    model.addConstr(numServers, GRB.EQUAL, numServersExpr, "nServers=sum(s[i])");

    final GRBLinExpr sumSW = sum(mergeArrays(w, s));
    model.addConstr(sumSW, GRB.EQUAL, n, "sum(w[i])+sum(s[i])=N");

    final GRBLinExpr sumModel = sum(m);
    model.addConstr(sumModel, GRB.EQUAL, mTotal, "sum(m[i])=M");
    final GRBLinExpr sumData = sum(d);
    model.addConstr(sumData, GRB.EQUAL, dTotal, "sum(d[i])=D");

    final GRBQuadExpr sumWD = linearCombination(w, d);
    model.addQConstr(sumWD, GRB.EQUAL, dTotal, "sum(w[i]*d[i])=D");

    final GRBQuadExpr sumSM = linearCombination(s, m);
    model.addQConstr(sumSM, GRB.EQUAL, mTotal, "sum(s[i]*m[i])=M");

    for (int i = 0; i < n; i++) {
      model.addConstr(w[i], GRB.GREATER_EQUAL, 0.0, String.format("w[%d]>=0", i));
      model.addConstr(s[i], GRB.GREATER_EQUAL, 0.0, String.format("s[%d]>=0", i));
      model.addConstr(d[i], GRB.GREATER_EQUAL, 0.0, String.format("d[%d]>=0", i));
      model.addConstr(m[i], GRB.GREATER_EQUAL, 0.0, String.format("m[%d]>=0", i));

      final GRBQuadExpr mutexExpr = new GRBQuadExpr();
      mutexExpr.addTerm(1.0, w[i], s[i]);
      model.addQConstr(mutexExpr, GRB.EQUAL, 0.0, String.format("w[%d]*s[%d]=0", i, i));

      final GRBQuadExpr nonZeroDForW = new GRBQuadExpr();
      nonZeroDForW.addTerm(1.0, d[i], w[i]);
      nonZeroDForW.addTerm(-1.0, w[i]);
      model.addQConstr(nonZeroDForW, GRB.GREATER_EQUAL, 0, String.format("w[%d]*(d[%d]-1)>=0", i, i));

      final GRBQuadExpr nonZeroWForD = new GRBQuadExpr();
      nonZeroWForD.addTerm(1.0, d[i], w[i]);
      nonZeroWForD.addTerm(-1.0, d[i]);
      model.addQConstr(nonZeroWForD, GRB.GREATER_EQUAL, 0, String.format("(w[%d]-1)*d[%d]>=0", i, i));

      final GRBQuadExpr nonZeroMForS = new GRBQuadExpr();
      nonZeroMForS.addTerm(1.0, m[i], s[i]);
      nonZeroMForS.addTerm(-1.0, s[i]);
      model.addQConstr(nonZeroMForS, GRB.GREATER_EQUAL, 0, String.format("s[%d]*(m[%d]-1)>=0", i, i));

      final GRBQuadExpr nonZeroSForM = new GRBQuadExpr();
      nonZeroSForM.addTerm(1.0, m[i], s[i]);
      nonZeroSForM.addTerm(-1.0, m[i]);
      model.addQConstr(nonZeroSForM, GRB.GREATER_EQUAL, 0, String.format("(s[%d]-1)*m[%d]>=0", i, i));
    }


    //////////////////////////////////
    /////    Cost definition     /////
    //////////////////////////////////
    final GRBVar maxCost = model.addVar(0.0, maxTotalCost, 0.0, GRB.CONTINUOUS, "maxCost");

    for (int i = 0; i < n; i++) {
      // comp_cost[i] = C_w_proc[i] * d[i]
      final GRBLinExpr compCostExpr = new GRBLinExpr();
      compCostExpr.addTerm(cWProc[i], d[i]);
      model.addConstr(compCost[i], GRB.EQUAL, compCostExpr, String.format("compCost[%d]=C_w_proc*d", i));

      final GRBVar maxTransferTime = model.addVar(0.0, maxCommCost, 0.0, GRB.CONTINUOUS, String.format("commCostW[%d]", i));

      // max_transfer_time[i] >= p * M / BW[i] * d[i] (case1: if worker i is the bottleneck)
      final GRBLinExpr transferTimeWExpr = new GRBLinExpr();
      transferTimeWExpr.addTerm((double) p * mTotal / bandwidth[i], d[i]);
      model.addConstr(maxTransferTime, GRB.GREATER_EQUAL, transferTimeWExpr, String.format("maxTransferTime[%d]>=p*M/BW*d[i]", i));

      // max_transfer_time[i] >= p * max(m[j]/BW[i, j])) * d[i] (case2: if server j is the bottleneck)
      for (int j = 0; j < n; j++) {
        if (i == j) {
          continue;
        }

        final GRBVar dImJ = model.addVar(0.0, dTotal * mTotal, 0.0, GRB.CONTINUOUS, String.format("dImJ(%d->%d)", i, j));
        mcCormickEnvelopes(model, dImJ, d[i], m[j],0.0, dTotal, 0.0, mTotal, "dImJ", i + "," + j);

        final GRBLinExpr transferTime = new GRBLinExpr();
        transferTime.addTerm((double) p / findMin(bandwidth, i, j), dImJ);
        model.addConstr(maxTransferTime, GRB.GREATER_EQUAL, transferTime, String.format("maxTransferTimeS>=transferTime[%d->%d]", i, j));
      }

      // comm_cost[i] = w[i] * max_transfer_time // communication cost matters only in workers
      commCost[i] = binaryMultVar(model, GRB.CONTINUOUS, w[i], maxTransferTime, maxCommCost, String.format("w*CommCost[%d]", i));

      // cost[i] = comp_cost[i] + comm_cost[i]
      final GRBLinExpr costSumExpr = new GRBLinExpr();
      costSumExpr.addTerm(1, compCost[i]);
      costSumExpr.addTerm(1, commCost[i]);
      model.addConstr(costI[i], GRB.EQUAL, costSumExpr, String.format("cost[%d]", i));

      // maxCost = max(cost[i])
      model.addConstr(maxCost, GRB.GREATER_EQUAL, costI[i], String.format("maxCost>=cost[%d]", i));
    }

    final GRBLinExpr totalCost = new GRBLinExpr();
    totalCost.addTerm(1.0, maxCost);
    model.setObjective(totalCost, GRB.MINIMIZE);

    // Optimize model
    model.optimize();

    System.out.println("============================================================");
    System.out.println("    Variables");
    System.out.println("============================================================");

    for (int i = 0; i < n; i++) {
      final String sb = w[i].get(GRB.StringAttr.VarName) + ' ' + Math.round(w[i].get(GRB.DoubleAttr.X)) + '\t' +
          s[i].get(GRB.StringAttr.VarName) + ' ' + Math.round(s[i].get(GRB.DoubleAttr.X));
      System.out.println(sb);

      final String sb2 = d[i].get(GRB.StringAttr.VarName) + ' ' + Math.round(d[i].get(GRB.DoubleAttr.X)) + '\t' +
          m[i].get(GRB.StringAttr.VarName) + ' ' + Math.round(m[i].get(GRB.DoubleAttr.X));
      System.out.println(sb2);

      final String sb3 = compCost[i].get(GRB.StringAttr.VarName) + ' ' + compCost[i].get(GRB.DoubleAttr.X) + '\t' +
          commCost[i].get(GRB.StringAttr.VarName) + ' ' + commCost[i].get(GRB.DoubleAttr.X) + '\t' +
          costI[i].get(GRB.StringAttr.VarName) + ' ' + costI[i].get(GRB.DoubleAttr.X);
      System.out.println(sb3);
      System.out.println();
    }

    final int[] wInt = convertToIntArray(w);
    final int[] dInt = convertToIntArray(d);
    final int[] sInt = convertToIntArray(s);
    final int[] mInt = convertToIntArray(m);

    System.out.println("============================================================");
    System.out.println("     Cost: " + model.get(GRB.DoubleAttr.ObjVal));
    System.out.println("============================================================");

    model.update();
    model.write("debug.lp");

    // Dispose of model and environment
    model.dispose();
    env.dispose();

    return new ConfDescriptor(dInt, mInt, wInt, sInt);
  }

  private int[] convertToIntArray(final GRBVar[] vars) throws GRBException {
    final int[] result = new int[vars.length];
    for (int i = 0; i < vars.length; i++) {
      result[i] = (int) Math.round(vars[i].get(GRB.DoubleAttr.X));
    }
    return result;
  }

  /**
   * @return A variable that denotes b * y, where b is a binary variable and y is a variable with a known upper bound.
   */
  private GRBVar binaryMultVar(final GRBModel model,
                                      final char type,
                                      final GRBVar binaryVar, final GRBVar var,
                                      final double upperBound, final String varName) throws GRBException {
    final GRBVar w = model.addVar(0.0, upperBound, 0.0, type, varName);

    final GRBLinExpr rhs1 = new GRBLinExpr();
    rhs1.addTerm(upperBound, binaryVar);
    model.addConstr(w, GRB.LESS_EQUAL, rhs1, String.format("%s<=u*x_j", varName));

    model.addConstr(w, GRB.GREATER_EQUAL, 0.0, String.format("%s>=0", varName));
    model.addConstr(w, GRB.LESS_EQUAL, var, String.format("%s<=y", varName));

    final GRBLinExpr rhs2 = new GRBLinExpr();
    rhs2.addTerm(upperBound, binaryVar);
    rhs2.addConstant(-upperBound);
    rhs2.addTerm(1.0, var);
    model.addConstr(w, GRB.GREATER_EQUAL, rhs2, String.format("%s>=U(x_j-1)+y", varName));

    return w;
  }

  /**
   * Adds a constraint of McCormick Envelopes.
   */
  private void mcCormickEnvelopes(final GRBModel model, final GRBVar auxVar,
                                         final GRBVar x, final GRBVar y,
                                         final double xL, final double xU,
                                         final double yL, final double yU,
                                         final String prefix, final String idx) throws GRBException {
    final GRBLinExpr rhs1 = new GRBLinExpr();
    rhs1.addTerm(xL, y);
    rhs1.addTerm(yL, x);
    rhs1.addConstant(-xL * yL);
    model.addConstr(auxVar, GRB.GREATER_EQUAL, rhs1, String.format("%s[%s]>=xLy+xyL-xLyL", prefix, idx));

    final GRBLinExpr rhs2 = new GRBLinExpr();
    rhs2.addTerm(xU, y);
    rhs2.addTerm(yU, x);
    rhs2.addConstant(-xU * yU);
    model.addConstr(auxVar, GRB.GREATER_EQUAL, rhs2, String.format("%s[%s]>=xUy+xyU-xUyU", prefix, idx));

    final GRBLinExpr rhs3 = new GRBLinExpr();
    rhs3.addTerm(xU, y);
    rhs3.addTerm(yL, x);
    rhs3.addConstant(-xU * yL);
    model.addConstr(auxVar, GRB.LESS_EQUAL, rhs3, String.format("%s[%s]<=xUy+xyL-xUyL", prefix, idx));

    final GRBLinExpr rhs4 = new GRBLinExpr();
    rhs4.addTerm(yU, x);
    rhs4.addTerm(xL, y);
    rhs4.addConstant(-xL * yU);
    model.addConstr(auxVar, GRB.LESS_EQUAL, rhs4, String.format("%s[%s]<=xyU+xLy-xLyU", prefix, idx));
  }

  /**
   * Computes (vars0[0] * vars1[0] + ... + vars0[i] * vars1[i] + ...).
   */
  private GRBQuadExpr linearCombination(final GRBVar[] vars0, final GRBVar[] vars1) throws GRBException {
    assert vars0.length == vars1.length;
    final double[] coefficients = new double[vars0.length];
    for (int i = 0; i < coefficients.length; i++) {
      coefficients[i] = 1.0;
    }

    final GRBQuadExpr expr = new GRBQuadExpr();
    expr.addTerms(coefficients, vars0, vars1);

    return expr;
  }

  /**
   * @return The linear expression that denotes the sum of variables.
   */
  private GRBLinExpr sum(final GRBVar[] vars) {
    final GRBLinExpr expr = new GRBLinExpr();
    for (final GRBVar var : vars) {
      expr.addTerm(1.0, var);
    }
    return expr;
  }

  /**
   * @return Merges multiple arrays of variables into one large array.
   */
  private GRBVar[] mergeArrays(final GRBVar[] ... arrays) {
    int numArrays = 0;
    for (final GRBVar[] array : arrays) {
      numArrays += array.length;
    }
    final GRBVar[] merged = new GRBVar[numArrays];
    int offset = 0;
    for (final GRBVar[] array : arrays) {
      System.arraycopy(array, 0, merged, offset, array.length);
      offset += array.length;
    }
    return merged;
  }

  /**
   * @return the minimum element in {@code arr}.
   */
  private double findMin(double[] arr) {
    double min = Double.MAX_VALUE;
    for (final double elem : arr) {
      if (elem < min) {
        min = elem;
      }
    }
    return min;
  }

  /**
   * @return the maximum element in {@code arr}.
   */
  private double findMax(double[] arr) {
    double max = Double.NEGATIVE_INFINITY;
    for (final double elem : arr) {
      if (elem > max) {
        max = elem;
      }
    }
    return max;
  }

  /**
   * @return the minimum element between arr[i] and arr[j].
   */
  private double findMin(double[] arr, int i, int j) {
    return Math.min(arr[i], arr[j]);
  }
}

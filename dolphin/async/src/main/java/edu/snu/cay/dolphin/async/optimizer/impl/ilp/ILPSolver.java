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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Computes Dolphin's optimal cost and configuration (w.r.t. w, s, d, m).
 */
final class ILPSolver {
  private static final Logger LOG = Logger.getLogger(ILPSolver.class.getName());

  private void optimize(final int n, final int dTotal, final int mTotal,
                        final int p, final double[] cWProc, final double[] bandwidth) throws GRBException {
    final String filename = String.format("solver-n%d-d%d-m%d-%d.log", n, dTotal, mTotal, System.currentTimeMillis());
    final GRBEnv env = new GRBEnv(filename);
    final GRBModel model = new GRBModel(env);
    model.set(GRB.DoubleParam.IntFeasTol, 1e-2);
    model.set(GRB.DoubleParam.MIPGap, 1e-1);

    final double maxCompCost = (double) dTotal * findMax(cWProc); // if the slowest worker computes all data
    final double maxCommCost = (double) n * p * mTotal * dTotal / findMin(bandwidth);
    // case 2: if the slowest server is a bottleneck
    final double maxTotalCost = maxCompCost + maxCommCost;
    System.out.println("Max compCost: " + maxCompCost + " Max commCost: " + maxCommCost);

    // Variables
    final int logD = log2(dTotal);
    final GRBVar[][] d = new GRBVar[n][logD+1];
    final GRBVar[] m = new GRBVar[n];
    final GRBVar[] w = new GRBVar[n];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j <= logD; j++) {
        d[i][j] = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("d[%d][%d]", i, j));
      }
      m[i] = model.addVar(0.0, mTotal, 0.0, GRB.INTEGER, String.format("m[%d]", i));
      w[i] = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("w[%d]", i));
    }

    // Constraints on the values.
    addConstraintsOnValues(model, n, mTotal, dTotal, w, m, d);

    final GRBVar maxCost = model.addVar(0.0, maxTotalCost, 0.0, GRB.CONTINUOUS, "maxCost");

    // Cost[i] = d[i] * (CwProc[i] + max(perBatchPullTime))

    // Compute max(p/bandwidth[j]*W*m[j])
    final GRBVar maxPullTimePerBatch = model.addVar((double) p / findMax(bandwidth), (double) p / findMin(bandwidth) * mTotal * n, 0.0, GRB.INTEGER, "maxPullTimePerBatch");

    // if a worker is the bottleneck
    final GRBLinExpr transferTimeWExpr = new GRBLinExpr();
    transferTimeWExpr.addConstant((double) p * mTotal / findMin(bandwidth));
    model.addConstr(maxPullTimePerBatch, GRB.GREATER_EQUAL, transferTimeWExpr, "maxTransferTime>=p*M/BW[i]");

    // if a server is the bottleneck
    for (int j = 0; j < n; j++) {
      final GRBLinExpr sumWIMJExpr = new GRBLinExpr();
      // Instead of multiplying W, we can accumulate w[i] * (p/bandwidth[j]*m[j]) in order to leave linear terms only
      for (int i = 0; i < n; i++) {
        if (i == j) {
          continue;
        }
        final GRBVar wImJ = binaryMultVar(model, GRB.INTEGER, w[i], m[j], mTotal, String.format("w[%d]m[%d]", i, j));
        sumWIMJExpr.addTerm((double) p / bandwidth[j], wImJ);
      }
      model.addConstr(maxPullTimePerBatch, GRB.GREATER_EQUAL, sumWIMJExpr, String.format("maxPullTimePerBatch>=p/bandwidth[%d]*W*m[%d]", j, j));
    }

    final GRBVar[] coeffSum = new GRBVar[n];
    for (int i = 0; i < n; i++) {

      coeffSum[i] = model.addVar(0.0, findMax(cWProc) + (double) p*mTotal/findMin(bandwidth)*(n-1), 0.0, GRB.CONTINUOUS, String.format("coeffSum[%d]", i));
      final GRBLinExpr costSumExpr = new GRBLinExpr();
      costSumExpr.addConstant(cWProc[i]);
      costSumExpr.addTerm(1.0, maxPullTimePerBatch);
      model.addConstr(coeffSum[i], GRB.EQUAL, costSumExpr, String.format("coeffSum[%d]=costSumExpr", i));
      final GRBQuadExpr costIExpr = new GRBQuadExpr();
      for (int j = 0; j < d[i].length; j++) {
        costIExpr.addTerm(Math.pow(2, j), coeffSum[i], d[i][j]);
      }

      model.addQConstr(maxCost, GRB.GREATER_EQUAL, costIExpr, String.format("maxCost>=cost[%d]", i));
    }

    final GRBLinExpr totalCost = new GRBLinExpr();
    totalCost.addTerm(1.0, maxCost);
    model.setObjective(totalCost, GRB.MINIMIZE);

    model.write("dolphin-cost.lp");
    final long startTimeMs = System.currentTimeMillis();
    model.setCallback(new DolphinSolverCallback(model, startTimeMs, n, w, d, m));

    // Optimize model
    model.optimize();

    final int status = model.get(GRB.IntAttr.Status);
    if (status == GRB.Status.INFEASIBLE) {
      onInfeasible(model);
    }



    printResult(model, startTimeMs, n, m, d);

    model.update();
    model.write("dolphin-cost-opt.lp");

    // Dispose of model and environment
    model.dispose();
    env.dispose();
  }

  private void onInfeasible(final GRBModel model) throws GRBException {
    model.computeIIS();
    final StringBuilder msgBuilder = new StringBuilder();
    for (GRBConstr c : model.getConstrs()) {
      if (c.get(GRB.IntAttr.IISConstr) == 1) {
        msgBuilder.append(c.get(GRB.StringAttr.ConstrName));
        msgBuilder.append('\n');
      }
    }
    LOG.log(Level.WARNING, "The following constraint(s) cannot be satisfied: {0}", msgBuilder.toString());
  }

  private void addConstraintsOnValues(final GRBModel model,
                                      final int n,
                                      final int mTotal,
                                      final int dTotal,
                                      final GRBVar[] w,
                                      final GRBVar[] m, final GRBVar[][] d) throws GRBException {
        final GRBVar numWorkers = model.addVar(1.0, n-1, 0.0, GRB.INTEGER,"W");
    final GRBLinExpr numWorkersExpr = sum(w);
    model.addConstr(numWorkers, GRB.EQUAL, numWorkersExpr, "nWorkers=sum(w[i])");

    final GRBVar numServers = model.addVar(1.0, n-1, 0.0, GRB.INTEGER,"S");
    final GRBLinExpr numServersExpr = new GRBLinExpr();
    numServersExpr.addConstant(n);
    numServersExpr.multAdd(-1.0, sum(w));
    model.addConstr(numServers, GRB.EQUAL, numServersExpr, "nServers=sum(s[i])");

    final GRBLinExpr sumModel = sum(m);
    model.addConstr(sumModel, GRB.EQUAL, mTotal, "sum(m[i])=M");
    final GRBLinExpr sumData = sum(d);
    model.addConstr(sumData, GRB.EQUAL, dTotal, "sum(d[i])=D");

    final GRBVar[] wIdI = binaryMultVarsSum(model, GRB.INTEGER, w, d, dTotal, "wIdI");
    final GRBLinExpr[] sImI = new GRBLinExpr[n];
    for (int i = 0; i < n; i++) {
      sImI[i] = new GRBLinExpr();
      sImI[i].addTerm(1.0, m[i]);
      sImI[i].addTerm(-1.0, binaryMultVar(model, GRB.INTEGER, w[i], m[i], mTotal, String.format("wm[%d]", i)));
    }

    final double[] coeff = new double[n];
    for (int i = 0; i < coeff.length; i++) {
      coeff[i] = 1.0;
    }

    final GRBLinExpr sumWD = new GRBLinExpr();
    sumWD.addTerms(coeff, wIdI);

    final GRBLinExpr sumSM = new GRBLinExpr();
    for (int i = 0; i < n; i++) {
      sumSM.add(sImI[i]);
    }

    model.addConstr(sumWD, GRB.EQUAL, dTotal, "sum(w[i]*d[i])=D");
    model.addConstr(sumSM, GRB.EQUAL, mTotal, "sum(s[i]*m[i])=M");
  }

  /**
   * @return A variable that denotes b * y, where b is a binary variable and y is a variable with a known upper bound.
   */
  private static GRBVar binaryMultVar(final GRBModel model,
                                      final char type,
                                      final GRBVar binaryVar, final GRBLinExpr var,
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
    rhs2.add(var);
    model.addConstr(w, GRB.GREATER_EQUAL, rhs2, String.format("%s>=U(x_j-1)+y", varName));

    return w;
  }

  /**
   * @return A variable that denotes b * y, where b is a binary variable and y is a variable with a known upper bound.
   */
  private static GRBVar binaryMultVar(final GRBModel model,
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
   * Computes (binaryVars[0] * vars[0] + ... + binaryVars[i] * vars[i] + ...).
   */
  private static GRBVar[] binaryMultVarsSum(final GRBModel model, final char type,
                                            final GRBVar[] binaryVars, final GRBVar[][] vars,
                                            final double upperBound, final String prefix) throws GRBException {
    assert binaryVars.length == vars.length;

    final GRBVar[] results = new GRBVar[binaryVars.length];
    for (int i = 0; i < binaryVars.length; i++) {
      final GRBLinExpr perNodeVal = binToExpr(vars[i]);
      final GRBVar result = binaryMultVar(model, type, binaryVars[i], perNodeVal, upperBound,
          String.format("%s[%d]", prefix, i));
      results[i] = result;
    }
    return results;
  }

  /**
   * @return The linear expression that denotes the sum of variables.
   */
  private static GRBLinExpr sum(final GRBVar[] vars) {
    final GRBLinExpr expr = new GRBLinExpr();
    for (final GRBVar var : vars) {
      expr.addTerm(1.0, var);
    }
    return expr;
  }

  private static GRBLinExpr sum(final GRBVar[][] vars) throws GRBException {
    final GRBLinExpr expr = new GRBLinExpr();
    for (final GRBVar[] perNodeVal : vars) {
      expr.add(binToExpr(perNodeVal));
    }
    return expr;
  }

  /**
   * @return The the value of a variable written in binary representation.
   */
  private static GRBLinExpr binToExpr(final GRBVar[] binRep) {
    final GRBLinExpr expr = new GRBLinExpr();
    for (int i = 0; i < binRep.length; i++) {
      expr.addTerm((int) Math.pow(2, i), binRep[i]);
    }
    return expr;
  }

  /**
   * @return the minimum element in {@code arr}.
   */
  private static double findMin(double[] arr) {
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
  private static double findMax(double[] arr) {
    double max = Double.NEGATIVE_INFINITY;
    for (final double elem : arr) {
      if (elem > max) {
        max = elem;
      }
    }
    return max;
  }

  private static int log2(int x) {
    return (int) (Math.log(x) / Math.log(2));
  }

  private static void printResult(final long startTimeMs,
                                  final double cost,
                                  final int[] mVal, final int[] dVal) throws GRBException {
       final long elapsedTime = System.currentTimeMillis() - startTimeMs;

    System.out.println(String.format("{\"time\": %d, \"cost\": %f, \"d\": %s, \"m\": %s}",
        elapsedTime, cost, encodeArray(dVal), encodeArray(mVal)));
  }

  private static String encodeArray(final int[] arr) {
    final StringBuilder sb = new StringBuilder().append('[');
    for (int i = 0; i < arr.length; i++) {
      sb.append(arr[i]);
      if (i != arr.length - 1) {
        sb.append(", ");
      }
    }
    sb.append(']');
    return sb.toString();
  }

  private static class DolphinSolverCallback extends GRBCallback {
    private final long startTimeMs;
    private final int n;
    private final GRBVar[] w, m;
    private final GRBVar[][] d;

    DolphinSolverCallback(final long startTimeMs, final int n, final GRBVar[] w, final GRBVar[][] d, final GRBVar[] m) {
      this.startTimeMs = startTimeMs;
      this.n = n;
      this.w = w;
      this.d = d;
      this.m = m;
    }

    @Override
    protected void callback() {
      try {
        if (where == GRB.CB_MIPSOL) {
          final long elapsedTimeMs = System.currentTimeMillis() - startTimeMs;
          final double cost = getDoubleInfo(GRB.CB_MIPSOL_OBJ);
          final int[] dVal = new int[n];
          final int[] mVal = new int[n];

          for (int i = 0; i < n; i++) {
            final double[] dDI = getSolution(d[i]);
            double val = 0;
            for (int j = 0; j < dDI.length; j++) {
              val += (1 << j) * dDI[j];
            }
            dVal[i] = (int) val;
            mVal[i] = (int) Math.round(getSolution(m)[i]);
          }

          printResult(elapsedTimeMs, cost, mVal, dVal);
        }
      } catch (GRBException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

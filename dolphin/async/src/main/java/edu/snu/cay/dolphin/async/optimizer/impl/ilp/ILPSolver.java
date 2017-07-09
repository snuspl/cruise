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

import javax.inject.Inject;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Computes Dolphin's optimal cost and configuration (w.r.t. w, s, d, m).
 */
public final class ILPSolver {
  private static final Logger LOG = Logger.getLogger(ILPSolver.class.getName());
  
  @Inject
  private ILPSolver() {
  }
  
  public ConfDescriptor optimize(final int n, final int dTotal, final int mTotal,
                                        final int p, final double[] cWProc, final double[] bandwidth) throws GRBException {
    final String filename = String.format("solver-n%d-d%d-m%d-%d.log", n, dTotal, mTotal, System.currentTimeMillis());
    LOG.log(Level.INFO, "p: {0}", p);
    LOG.log(Level.INFO, "cWProc: {0}", Arrays.toString(cWProc));
    LOG.log(Level.INFO, "BandWidth: {0}", Arrays.toString(bandwidth));
    LOG.log(Level.INFO, "mTotal: {0}, dTotal: {1}", new Object[]{mTotal, dTotal});
    
    // Gurobi environment configurations
    final GRBEnv env = new GRBEnv(filename);
    final GRBModel model = new GRBModel(env);
    model.set(GRB.DoubleParam.IntFeasTol, 1e-2);
    model.set(GRB.DoubleParam.MIPGap, 1e-1);
    model.set(GRB.IntParam.Threads, 8);
    
    // Variables
    final GRBVar[] m = new GRBVar[n];
    final GRBVar[] s = new GRBVar[n];
    final GRBVar[] t = new GRBVar[n];
    final GRBVar[][] sImJ = new GRBVar[n][n];
    
    for (int i = 0; i < n; i++) {
      m[i] = model.addVar(0.0, mTotal, 0.0, GRB.INTEGER, String.format("m[%d]", i));
      s[i] = model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("s[%d]", i));
      t[i] = model.addVar(0.0, 1.0, 0.0, GRB.CONTINUOUS, String.format("t[%d]", i));
    }
    
    // For fixed i, calculate sum_{j}( 1 / min(bandwidth[i], bandwidth[j]) )
    final double[] bandwidthHarmonicSum = new double[n];
    computeBandwidthHarmonicSum(bandwidthHarmonicSum, bandwidth, n);
    
    // basicConstraints function includes the following constraints.
    // 1. Sum(m[i]) = M
    // 2. Define s[i]*m[j] as a gurobi variable.
    // 3. m[i] == 0 iff s[i] == 0
    basicConstraints(model, m, mTotal, n, sImJ, s);
    
    // maxCommCost occurs when there is only one server and server is bottleneck for communication cost.
    final double maxCommCost = (double) n * mTotal * p / findMin(bandwidth);
    final double normalizationTerm = (1 << 7) / maxCommCost;
    final int logMaxCommCost = 7;
    
    // Express maxPullTimePerBatch with binary.
    final GRBVar[][] maxPullTimePerBatch = new GRBVar[n][logMaxCommCost + 1];
    for (int j = 0; j < n; j++) {
      for (int i = 0; i <= logMaxCommCost; i++) {
        maxPullTimePerBatch[j][i] =
            model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("maxPullTimePerBatch[%d][%d]", j, i));
      }
    }
    
    final GRBLinExpr[] workerBottleneck = new GRBLinExpr[n];
    // if a worker is the bottleneck
    for (int i = 0; i < n; i++) {
      workerBottleneck[i] = new GRBLinExpr();
      for (int j = 0; j < n; j++) {
        workerBottleneck[i].addTerm(normalizationTerm * p / Math.min(bandwidth[i], bandwidth[j]), m[j]);
      }
      model.addConstr(binToExpr(maxPullTimePerBatch[i]), GRB.GREATER_EQUAL, workerBottleneck[i],
          String.format("maxTransferTime>=Sigma(p*m[j]/min(BW[%d], BW[j]))", i));
    }
    
    // if a server is the bottleneck
    final GRBVar serverBottleneck =
        model.addVar(0.0, normalizationTerm * mTotal * p * n / findMin(bandwidth), 0.0,
            GRB.CONTINUOUS, "serverBottlenectCost");
    
    final GRBLinExpr[] sumWIMJExpr = new GRBLinExpr[n];
    for (int j = 0; j < n; j++) {
      sumWIMJExpr[j] = new GRBLinExpr();
      sumWIMJExpr[j].addTerm(normalizationTerm * p * bandwidthHarmonicSum[j], m[j]);
      for (int i = 0; i < n; i++) {
        sumWIMJExpr[j].addTerm(normalizationTerm * -p / Math.min(bandwidth[i], bandwidth[j]), sImJ[i][j]);
      }
      model.addConstr(serverBottleneck, GRB.GREATER_EQUAL, sumWIMJExpr[j],
          String.format("serverBottlenectCost>=W*m[%d]*p/bandwidth[%d]", j, j));
    }
    
    final GRBVar[] forServerBottleneck = new GRBVar[n];
    final GRBVar[] forServerBottleneckBin = new GRBVar[n];
    final GRBQuadExpr forServerBottleneckExpr = new GRBQuadExpr();
    final GRBLinExpr forServerBottleneckBinExpr = new GRBLinExpr();
    for (int i = 0; i < n; i++) {
      forServerBottleneck[i] = model.addVar(0.0, normalizationTerm * mTotal * p * n / findMin(bandwidth), 0.0,
          GRB.CONTINUOUS, String.format("forServerBottleneck[%d]", i));
      model.addConstr(sumWIMJExpr[i], GRB.EQUAL, forServerBottleneck[i], String.format("forServerBottleneck[%d]==sumWIMJExpr[%d]", i, i));
      forServerBottleneckBin[i] =
          model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("forServerBottleneckBin[%d]", i));
      forServerBottleneckExpr.addTerm(1.0, forServerBottleneck[i], forServerBottleneckBin[i]);
      forServerBottleneckBinExpr.addTerm(1.0, forServerBottleneckBin[i]);
    }
    model.addQConstr(forServerBottleneckExpr, GRB.GREATER_EQUAL, serverBottleneck,
        "serverBottleneck<Sigma(bin*serverCost");
    model.addConstr(forServerBottleneckBinExpr, GRB.EQUAL, 1.0, "serverBottleneckBinSigma");
    
    for (int  i = 0; i < n; i++) {
      model.addConstr(binToExpr(maxPullTimePerBatch[i]), GRB.GREATER_EQUAL, serverBottleneck,
          String.format("maxPullTimePerBatch>=p/bandwidth[%d]*W*m[%d]", i, i));
    }
    
    for (int i = 0; i < n; i++) {
      final GRBVar forMaxConstr = model.addVar(0.0, normalizationTerm * p * mTotal / findMin(bandwidth), 0.0,
          GRB.CONTINUOUS, String.format("forMaxConstr[%d]", i));
      final GRBVar[] forMaxConstrBin = new GRBVar[2];
      final GRBQuadExpr maxConstr = new GRBQuadExpr();
      final GRBLinExpr binConstr = new GRBLinExpr();
      for (int j = 0; j < 2; j++) {
        forMaxConstrBin[j] =
            model.addVar(0.0, 1.0, 0.0, GRB.BINARY, String.format("forMaxConstrBin[%d][%d]", i, j));
        binConstr.addTerm(1.0, forMaxConstrBin[j]);
      }
      model.addConstr(forMaxConstr, GRB.EQUAL, workerBottleneck[i],
          String.format("forMaxConstr[%d]==workerBottleneck[%d]", i, i));
      maxConstr.addTerm(1.0, forMaxConstrBin[0], forMaxConstr);
      maxConstr.addTerm(1.0, forMaxConstrBin[1], serverBottleneck);
      model.addQConstr(maxConstr, GRB.GREATER_EQUAL, binToExpr(maxPullTimePerBatch[i]),
          String.format("maxPullTimePerBatch[%d]<=Sigma(forMaxConstrBin[%d]*forMaxConstr[%d]", i, i, i));
      model.addConstr(binConstr, GRB.EQUAL, 1.0, String.format("Sigma[%d](forMaxConstrBin)=1", i));
    }
    
    // cost[i]*t[i] = 1
    final GRBQuadExpr[] costItI = new GRBQuadExpr[n];
    for (int i = 0; i < n; i++) {
      costItI[i] = new GRBQuadExpr();
      for (int j = 0; j <= logMaxCommCost; j++) {
        costItI[i].addTerm(Math.pow(2, j), t[i], maxPullTimePerBatch[i][j]);
      }
      costItI[i].addTerm(cWProc[i], t[i]);
      model.addQConstr(costItI[i], GRB.EQUAL, 1, String.format("cost[%d]*t[%d]=1", i, i));
    }
    
    // Want to maximize Sigma(t[i])
    final GRBQuadExpr sumT = new GRBQuadExpr();
    for (int i = 0; i < n; i++) {
      sumT.addTerm(1.0, t[i]);
      sumT.addTerm(-1.0, t[i], s[i]);
    }
    model.setObjective(sumT, GRB.MAXIMIZE);
    
    model.write("dolphin-cost.lp");
    final long startTimeMs = System.currentTimeMillis();
    model.setCallback(new DolphinSolverCallback(startTimeMs, n, m));
    
    // Optimize model
    model.optimize();
    
    final int status = model.get(GRB.IntAttr.Status);
    if (status == GRB.Status.INFEASIBLE) {
      onInfeasible(model);
      return null;
    }
    
    final double cost = model.get(GRB.DoubleAttr.ObjVal);
    final int[] mVal = new int[n];
    final int[] dVal = new int[n];
    final int[] wVal = new int[n];
    
    computeMDWvalues(mVal, dVal, wVal, m, s, n, bandwidth, cWProc, p, dTotal);
    
    printResult(startTimeMs, cost, mVal);
    
    model.update();
    model.write("dolphin-cost-opt.lp");
    
    // Dispose of model and environment
    model.dispose();
    env.dispose();
    
    LOG.log(Level.INFO, "dVal : " + encodeArray(dVal));
    LOG.log(Level.INFO, "wVal : " + encodeArray(wVal));
    
    return new ConfDescriptor(dVal, mVal, wVal, cost);
  }
  
  private static void computeMDWvalues(final int[] mVal, final int[] dVal, final int[] wVal, final GRBVar[] m,
                                       final GRBVar[] s, final int n, final double[] bandwidth, final double[] cWProc,
                                       final int p, final int dTotal) throws GRBException {
    for (int i = 0; i < n; i++) {
      mVal[i] = (int) Math.round(m[i].get(GRB.DoubleAttr.X));
      wVal[i] = 1 - (int) Math.round(s[i].get(GRB.DoubleAttr.X));
    }
    
    // when server is bottleneck
    double commCostServer = 0.0;
    for (int j = 0; j < n; j++) {
      if (mVal[j] == 0) {
        continue;
      }
      double commCostServerCandidate = 0.0;
      for (int i = 0; i < n; i++) {
        if (mVal[i] == 0) {
          commCostServerCandidate += (double) mVal[j] * p / Math.min(bandwidth[i], bandwidth[j]);
        }
      }
      if (commCostServer < commCostServerCandidate) {
        commCostServer = commCostServerCandidate;
      }
    }
    
    final double[] estimatedCost = new double[n];
    for (int i = 0; i < n; i++) {
      if (mVal[i] != 0) {
        continue;
      }
      double commCostWorker = 0.0;
      for (int j = 0; j < n; j++) {
        if (mVal[j] != 0) {
          commCostWorker += (double) mVal[j] * p / Math.min(bandwidth[i], bandwidth[j]);
        }
      }
      estimatedCost[i] = cWProc[i] + Math.max(commCostWorker, commCostServer);
    }
    
    double harmonicEstimatedCostSum = 0.0;
    for (int i = 0; i < n; i++) {
      if (mVal[i] != 0) {
        continue;
      }
      harmonicEstimatedCostSum += 1.0 / estimatedCost[i];
    }
    
    // Determine mVal[i] proportional to the 1 / estimatedCost[i]
    for (int i = 0; i < n; i++) {
      if (mVal[i] != 0) {
        dVal[i] = 0;
      } else {
        dVal[i] = (int) ((double) dTotal / estimatedCost[i] / harmonicEstimatedCostSum);
      }
    }
    
    int sumD = 0;
    for (int i = 0; i < n; i++) {
      sumD += dVal[i];
    }
    int diff = 0;
    if (sumD < dTotal) {
      diff = dTotal - sumD;
    }
    while (diff != 0) {
      for (int i = 0; i < n; i++) {
        if (dVal[i] != 0) {
          dVal[i]++;
          diff--;
        }
        if (diff == 0) {
          break;
        }
      }
    }
  }
  
  private void basicConstraints(final GRBModel model, final GRBVar[] m, final int mTotal, final int n,
                                       final GRBVar[][] sImJ, final GRBVar[] s) throws GRBException {
    // Sum(m[i])=M
    final GRBLinExpr sumModel = sum(m);
    model.addConstr(sumModel, GRB.EQUAL, mTotal, "sum(m[i])=M");
    
    // Define sImJ
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        sImJ[i][j] = binaryMultVar(model, GRB.INTEGER, s[i], m[j], mTotal, String.format("s[%d]m[%d]", i, j));
      }
    }
    
    // m[i] == 0 iff s[i] == 0
    final GRBLinExpr sumSM = new GRBLinExpr();
    for (int i = 0; i < n; i++) {
      sumSM.addTerm(1.0, sImJ[i][i]);
    }
    model.addConstr(sumSM, GRB.EQUAL, mTotal, "sum(s[i]*m[i])=M");
  }
  
  private static void computeBandwidthHarmonicSum(final double[] bandwidthHarmonicSum, final double[] bandwidth, final int n) {
    for (int i = 0; i < n; i++) {
      bandwidthHarmonicSum[i] = 0;
      for (int j = 0; j < n; j++) {
        bandwidthHarmonicSum[i] += 1.0 / Math.min(bandwidth[i], bandwidth[j]);
      }
    }
  }
  
  private void onInfeasible(final GRBModel model) throws GRBException {
    model.computeIIS();
    final StringBuilder msgBuilder = new StringBuilder();
    for (final GRBConstr c : model.getConstrs()) {
      if (c.get(GRB.IntAttr.IISConstr) == 1) {
        msgBuilder.append(c.get(GRB.StringAttr.ConstrName));
        msgBuilder.append('\n');
      }
    }
    System.out.println("The following constraint(s) cannot be satisfied: " + msgBuilder.toString());
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
   * @return The linear expression that denotes the sum of variables.
   */
  private static GRBLinExpr sum(final GRBVar[] vars) {
    final GRBLinExpr expr = new GRBLinExpr();
    for (final GRBVar var : vars) {
      expr.addTerm(1.0, var);
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
  private static double findMin(final double[] arr) {
    double min = Double.MAX_VALUE;
    for (final double elem : arr) {
      if (elem < min) {
        min = elem;
      }
    }
    return min;
  }
  
  private static void printResult(final long startTimeMs,
                                  final double cost,
                                  final int[] mVal) throws GRBException {
    final double elapsedTime = (System.currentTimeMillis() - startTimeMs) / 1000.0D;
    System.out.println("Cost: time: " + elapsedTime + ", cost: " + cost);
    System.out.println("mVal : " + encodeArray(mVal));
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
    private final GRBVar[] m;
    
    DolphinSolverCallback(final long startTimeMs, final int n, final GRBVar[] m) {
      this.startTimeMs = startTimeMs;
      this.n = n;
      this.m = m;
    }
    
    @Override
    protected void callback() {
      try {
        if (where == GRB.CB_MIPSOL) {
          final long elapsedTimeMs = System.currentTimeMillis() - startTimeMs;
          final double cost = getDoubleInfo(GRB.CB_MIPSOL_OBJ);
          final int[] mVal = new int[n];
          
          for (int i = 0; i < n; i++) {
            mVal[i] = (int) Math.round(getSolution(m)[i]);
          }
          
          printResult(elapsedTimeMs, cost, mVal);
        }
      } catch (GRBException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

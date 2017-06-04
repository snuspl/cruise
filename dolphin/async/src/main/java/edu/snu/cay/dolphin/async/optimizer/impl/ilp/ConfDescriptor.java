package edu.snu.cay.dolphin.async.optimizer.impl.ilp;

/**
 * Created by yunseong on 6/4/17.
 */
final class ConfDescriptor {
  private final int[] d;
  private final int[] m;
  private final int[] w;
  private final int[] s;

  ConfDescriptor(final int[] d, final int[] m, final int[] w, final int[] s) {
    this.d = d;
    this.m = m;
    this.w = w;
    this.s = s;
  }

  private int[] getD() {
    return d;
  }

  private int[] getM() {
    return m;
  }

  private int[] getW() {
    return w;
  }

  private int[] getS() {
    return s;
  }
}

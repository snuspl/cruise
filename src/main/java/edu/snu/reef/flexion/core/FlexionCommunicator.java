package edu.snu.reef.flexion.core;

public interface FlexionCommunicator {
  public void send(Integer data) throws Exception;
  public Integer receive() throws Exception;
  public boolean terminate() throws Exception;
}

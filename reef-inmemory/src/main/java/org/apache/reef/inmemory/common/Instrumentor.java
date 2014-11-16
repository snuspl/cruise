package org.apache.reef.inmemory.common;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(InstrumentorImpl.class)
public interface Instrumentor {
  public Configuration getConfiguration();
}
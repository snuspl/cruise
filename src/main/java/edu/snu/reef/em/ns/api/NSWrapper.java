package edu.snu.reef.em.ns.api;

import edu.snu.reef.em.ns.impl.NSWrapperImpl;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(NSWrapperImpl.class)
public interface NSWrapper<T> {
  NetworkService<T> getNetworkService();
}
package edu.snu.reef.em.driver;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.IntRange;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.List;
import java.util.Set;

@DriverSide
public class ElasticMemoryServiceImpl implements ElasticMemoryService {

  @Override
  public void add(int number, int megaBytes, int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void delete(String evalId) {
    throw new NotImplementedException();
  }

  @Override
  public void resize(String evalId, int megaBytes, int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void move(String dataClassName, Set<IntRange> rangeSet, String srcEvalId, String destEvalId) {
    throw new NotImplementedException();
  }

  @Override
  public void checkpoint(String evalId) {
    throw new NotImplementedException();
  }
}

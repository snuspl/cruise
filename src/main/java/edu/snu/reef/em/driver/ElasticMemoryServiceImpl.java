package edu.snu.reef.em.driver;

import org.apache.reef.annotations.audience.DriverSide;

import java.util.List;

@DriverSide
public class ElasticMemoryServiceImpl implements ElasticMemoryService {

//  @Override
//  public void add(int size) {
//    // request new evaluator
//    // add task to comm group
//    // submit task for evaluator
//  }
//
//  @Override
//  public void del(String evalId) {
//    // quit task on specific evaluator
//    // del task from comm group
//    // release specified evaluator
//  }
//
//  @Override
//  public void resize(String evalId, int size) {
//    throw new UnsupportedOperationException("Resizing evaluator is not supported");
//  }
//
//  @Override
//  public void move(Partition partition, String destEvalId) {
//    // update dataMap
//
//    // migrate data
//  }
//
//  @Override
//  public void merge(List<Partition> partitions, String destEvalId) {
//    Partition mergedPartition = DataMerger.merge(partitions);
//
//    // add, del, move
//  }
//
//  @Override
//  public void split(Partition partition, List<String> destEvalIds, HashFunc func) {
//
//    int splitNum = destEvalIds.size();
//
//    List<NumberedPartition> splittedStates = DataSplitter.split(partition, splitNum, func);
//
//    // add, del, move
//  }
}

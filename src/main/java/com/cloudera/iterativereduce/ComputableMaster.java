package com.cloudera.iterativereduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;


public interface ComputableMaster<T extends Updateable> {
  void setup(Configuration c);
  void complete(DataOutputStream out) throws IOException;
  T compute(Collection<T> workerUpdates, Collection<T> masterUpdates);
  T getResults();
}
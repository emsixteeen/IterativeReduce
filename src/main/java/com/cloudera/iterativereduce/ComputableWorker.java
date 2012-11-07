package com.cloudera.iterativereduce;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.iterativereduce.io.RecordParser;

public interface ComputableWorker<T extends Updateable> {
  void setup(Configuration c);
  T compute(List<T> records);
  T compute();
  // dont know a better way to do this currently
  void setRecordParser(RecordParser r);
  T getResults();
  void update(T t);
}

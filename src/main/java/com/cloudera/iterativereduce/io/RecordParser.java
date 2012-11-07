package com.cloudera.iterativereduce.io;

import com.cloudera.iterativereduce.Updateable;

public interface RecordParser<T extends Updateable> {
  void reset();
  void parse();
  void setFile(String file, long offset, long length);
  void setFile(String file);
  boolean hasMoreRecords();
  T nextRecord();
  int getCurrentRecordsProcessed();
}
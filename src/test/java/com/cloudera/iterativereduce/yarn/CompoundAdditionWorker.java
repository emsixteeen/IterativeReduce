package com.cloudera.iterativereduce.yarn;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.cloudera.iterativereduce.ComputableWorker;
import com.cloudera.iterativereduce.io.RecordParser;
import com.cloudera.iterativereduce.io.TextRecordParser;

/*
 * Useless standalone, used for tests
 */
public class CompoundAdditionWorker implements ComputableWorker<UpdateableInt> {
  private static final Log LOG = LogFactory.getLog(CompoundAdditionWorker.class);
  
  int masterTotal = 0;
  UpdateableInt workerTotal = new UpdateableInt();
  TextRecordParser rp;
  Text t = new Text();
    
  
  @Override
  public UpdateableInt compute(List<UpdateableInt> records) {
    return null;
  }
  
  public UpdateableInt getResults() {
    return workerTotal;
  }

  @Override
  public void update(UpdateableInt t) {
    masterTotal = t.get();
  }

  @Override
  public void setup(Configuration c) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public UpdateableInt compute() {
    int total = 0;

    try { 
      while (rp.next(t)) {
        int j = Integer.parseInt(t.toString());
        total += j; 
      }
    } catch (IOException ex) {
      LOG.warn(ex);
    }

    workerTotal.set(masterTotal + total);
    return workerTotal;
  }

  @Override
  public void setRecordParser(RecordParser r) {
    rp = (TextRecordParser)r;
  }

  @Override
  public boolean IncrementIteration() {
    return false;
  }


}


package com.cloudera.iterativereduce.yarn;

import java.nio.ByteBuffer;

import com.cloudera.iterativereduce.Updateable;

/*
 * Useless standalone, used for tests
 */
public class UpdateableInt implements Updateable<Integer> {
  private int i = 0;
  private ByteBuffer b;
  private int IterationNumber = 0;
  private int BatchNumber = 0;
  
  @Override
  public ByteBuffer toBytes() {
    if (b == null)
      b = ByteBuffer.allocate(4);
    
    b.clear();
    b.putInt(i);
    b.rewind();
    
    return b;
    
  }

  @Override
  public void fromBytes(ByteBuffer b) {
    i = b.getInt();
  }
  
  @Override
  public void fromString(String s) {
    i = Integer.parseInt(s);
  }
  
  @Override
  public Integer get() {
    return i;
  }

  @Override
  public void set(Integer t) {
    i = t;
  }
  
/*  @Override
  public void setIterationState(int IterationNumber, int BatchNumber) {
    
    this.IterationNumber = IterationNumber;
    this.BatchNumber = BatchNumber;
    
  }
  
  @Override
  public int getGlobalIterationNumber() {
    return this.IterationNumber;
  }
  
  @Override
  public int getGlobalBatchNumber() {
    return this.BatchNumber;
  }
*/  
  
}

package com.cloudera.iterativereduce.yarn.appmaster;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.iterativereduce.ComputableMaster;
import com.cloudera.iterativereduce.Utils;
import com.cloudera.iterativereduce.yarn.CompoundAdditionMaster;
import com.cloudera.iterativereduce.yarn.UpdateableInt;
import com.cloudera.iterativereduce.yarn.avro.generated.FileSplit;
import com.cloudera.iterativereduce.yarn.avro.generated.StartupConfiguration;
import com.cloudera.iterativereduce.yarn.avro.generated.WorkerId;


public class ErroneousMasterShutdownTest {

  private ApplicationMasterService<UpdateableInt> masterService;
  
  @Before
  public void setUp() throws Exception {
    FileSplit split = FileSplit.newBuilder().setPath("/foo/bar").setOffset(100)
        .setLength(200).build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(2).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);
    workers.put(Utils.createWorkerId("worker2"), conf);

    InetSocketAddress masterAddress = new InetSocketAddress(9999);
    ComputableMaster<UpdateableInt> computableMaster = new CompoundAdditionMaster();
    
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);
  }
  
  @Test
  public void testErroneousShutdownViaShutdown() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    Future<Integer> master = pool.submit(masterService);
    Thread.sleep(500);
    
    pool.shutdownNow();
    assertEquals(Integer.valueOf(-1), master.get());
  }
}

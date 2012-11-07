package com.cloudera.iterativereduce.yarn.appworker;

import static org.junit.Assert.assertEquals;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.iterativereduce.ComputableMaster;
import com.cloudera.iterativereduce.ComputableWorker;
import com.cloudera.iterativereduce.Utils;
import com.cloudera.iterativereduce.io.TextRecordParser;
import com.cloudera.iterativereduce.yarn.CompoundAdditionMaster;
import com.cloudera.iterativereduce.yarn.CompoundAdditionWorker;
import com.cloudera.iterativereduce.yarn.UpdateableInt;
import com.cloudera.iterativereduce.yarn.appmaster.ApplicationMasterService;
import com.cloudera.iterativereduce.yarn.avro.generated.FileSplit;
import com.cloudera.iterativereduce.yarn.avro.generated.StartupConfiguration;
import com.cloudera.iterativereduce.yarn.avro.generated.WorkerId;

public class SingleWorkerServiceTest {

  InetSocketAddress masterAddress;
  ExecutorService pool;

  private ApplicationMasterService<UpdateableInt> masterService;
  private Future<Integer> master;
  private ComputableMaster<UpdateableInt> computableMaster;

  private ApplicationWorkerService<UpdateableInt> workerService;
  private ComputableWorker<UpdateableInt> computableWorker;

  @Before
  public void setUp() throws Exception {
    masterAddress = new InetSocketAddress(9999);
    pool = Executors.newFixedThreadPool(2);

    setUpMaster();
  }

  @Before
  public void setUpFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localFs = FileSystem.getLocal(conf);
    Path testDir = new Path("testData");
    Path inputFile = new Path(testDir, "testWorkerService.txt");

    Writer writer = new OutputStreamWriter(localFs.create(inputFile, true));
    writer.write("10\n20\n30\n40\n50\n60\n70\n80\n90\n100");
    writer.close();
  }
  
  @After
  public void cleanup() {
    pool.shutdown();
  }

  public void setUpMaster() throws Exception {
    FileSplit split = FileSplit.newBuilder()
        .setPath("testData/testWorkerService.txt").setOffset(0).setLength(200)
        .build();

    StartupConfiguration conf = StartupConfiguration.newBuilder()
        .setSplit(split).setBatchSize(200).setIterations(1).setOther(null)
        .build();

    HashMap<WorkerId, StartupConfiguration> workers = new HashMap<WorkerId, StartupConfiguration>();
    workers.put(Utils.createWorkerId("worker1"), conf);

    computableMaster = new CompoundAdditionMaster();
    masterService = new ApplicationMasterService<UpdateableInt>(masterAddress,
        workers, computableMaster, UpdateableInt.class);

    master = pool.submit(masterService);
  }

  @Test
  public void testWorkerService() throws Exception {
    TextRecordParser<UpdateableInt> parser = new TextRecordParser<UpdateableInt>();
    computableWorker = new CompoundAdditionWorker();
    workerService = new ApplicationWorkerService<UpdateableInt>(
        "worker1", masterAddress, parser, computableWorker, UpdateableInt.class);

    assertEquals(0, workerService.run());
    assertEquals(Integer.valueOf(0), master.get());
    
    // Bozo numbers
    assertEquals(Integer.valueOf(550), computableWorker.getResults().get());
    assertEquals(Integer.valueOf(1100), computableMaster.getResults().get());
  }
  
  public static void main(String[] args) throws Exception {
    SingleWorkerServiceTest tsws = new SingleWorkerServiceTest();
    tsws.setUp();
    tsws.testWorkerService();
  }
}

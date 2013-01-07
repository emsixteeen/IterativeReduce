package com.cloudera.iterativereduce.yarn.appmaster;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.junit.Test;

import com.cloudera.iterativereduce.ConfigFields;
import com.cloudera.iterativereduce.yarn.appmaster.ApplicationMaster.ConfigurationTuple;
import com.cloudera.iterativereduce.yarn.avro.generated.FileSplit;
import com.cloudera.iterativereduce.yarn.avro.generated.StartupConfiguration;


public class TestInputSplitMechanics {

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }  
  
  private static Path input_file = new Path( "/Users/jpatterson/Downloads/datasets/20news-kboar/models/input/kboar_4_blocks.txt" );

  public void listDir( String str_path ) throws IOException {

    System.out.println( "List Path> " + str_path );
    
    boolean good_path = true;
    Path p = null;
    try {
      p = new Path( str_path );
    //} catch(java.lang.IllegalArgumentException e) {
    } catch(Exception e) {
      good_path = false;
    }
    
    //FileStatus f = localFs.getFileStatus(p);
    
    
    if ( good_path ) {
      //FileStatus f = localFs.getFileStatus(p);
    
      if ( localFs.exists(p) == false ) {
        good_path = false;
        System.out.println( "\tPath doesnt exist!" );
      }
      
      
      for (FileStatus child_fst : localFs.listStatus(p)) {
        
        System.out.println( "\t> Dir: " + child_fst.isDirectory() + ", Path: " + child_fst.getPath().toString() );
  
      } // for
      
    } // if
    
    System.out.println( "List Done\n\n" );
    
  }
  

  
  private HashSet<FileSplit> getSplitsForPath( String str_path ) throws IOException {
    
    HashSet<FileSplit> splits = new HashSet<FileSplit>();
   
    
    System.out.println( "\t\tgetSplitsForPath> " + str_path );
    
    boolean good_path = true;
    Path p = null;
    try {
      p = new Path( str_path );
    //} catch(java.lang.IllegalArgumentException e) {
    } catch(Exception e) {
      good_path = false;
    }
    
    //FileStatus f = localFs.getFileStatus(p);
    
    
    if ( good_path ) {
      //FileStatus f = localFs.getFileStatus(p);
    
      if ( localFs.exists(p) == false ) {
        //good_path = false;
        //System.out.println( "\tPath doesnt exist!" );
        return splits;
      }
      
      FileStatus file_status = localFs.getFileStatus(p);
      
      for (FileStatus child_fst : localFs.listStatus(p)) {
        
        System.out.println( "\t\t\t> Dir: " + child_fst.isDirectory() + ", Path: " + child_fst.getPath().toString() );
  
        if (false == child_fst.isDirectory() ) {
          
          BlockLocation[] bl = localFs.getFileBlockLocations(child_fst.getPath(), 0, child_fst.getLen());
          
          System.out.println( "\t\t\tfound blocks: " + bl.length );
  
          for (BlockLocation b : bl) {
//            FileSplit split = FileSplit.newBuilder().setPath(p.toString())
//                .setOffset(b.getOffset()).setLength(b.getLength()).build();
          FileSplit split = FileSplit.newBuilder().setPath(child_fst.getPath().toString())
          .setOffset(b.getOffset()).setLength(b.getLength()).build();
          
          splits.add( split );
  
            System.out.println( "\t\t\tsplit: " + split.toString() );

          } // for
          
          
        }
        
      } // for
      
    } // if    
    
    return splits;
    
  }
  
  
  
  
  
  
  /**
   * First take a look at the files in the string, to see if there are multiple paths
   * 
   * @param paths_string
   * @return
   * @throws IOException 
   */
  public HashSet<FileSplit> getFileSplits_Proto( String paths_string ) throws IOException {
    
    HashSet<FileSplit> splits = new HashSet<FileSplit>();
    
    String[] found_paths = paths_string.split(",");
    
    System.out.println( "> getFileSplits_Proto: Found Paths: " + found_paths.length );
    
    for ( int x = 0; x < found_paths.length; x++ ) {
      System.out.println( "\t> Path: " + found_paths[x] );
      
      HashSet<FileSplit> partial_splits = getSplitsForPath( found_paths[x] ); 
      splits.addAll(partial_splits);

    } // for each path found
    
    return splits;
  }
  
  /**
   * This is a simulated version of how ApplicationMaster does it
   * 
   * --- there was an issue with some of the Avro stuff based on how the enclosing type did some things.
   * 
   * @param paths
   * @return
   * @throws IOException
   */
  public HashSet<ConfigurationTuple> getSplits( String paths ) throws IOException {
    
    System.out.println( "getSplits: " + paths );
    
    HashSet<ConfigurationTuple> configTuples = new HashSet<ConfigurationTuple>();
    
    //Path p = new Path(props.getProperty(ConfigFields.APP_INPUT_PATH));
    
    ArrayList<String> paths_found = new ArrayList<String>();
    String potential_paths = paths;

    String[] found_paths = potential_paths.split(",");

    int workerId = 0;
    
    Properties props = new Properties();
//    props.load(new FileInputStream(ConfigFields.APP_CONFIG_FILE)); // Should be
  
    props.setProperty(ConfigFields.APP_INPUT_PATH, input_file.toString());
    Map<CharSequence, CharSequence> appConfig = new HashMap<CharSequence, CharSequence>();
    
    for (Map.Entry<Object, Object> prop : props.entrySet()) {
      appConfig.put((String) prop.getKey(), (String) prop.getValue());
    }

    boolean good_path = true;
    
    for ( int x = 0; x < found_paths.length; x++ ) {
      System.out.println( "\tPath> " + found_paths[x] );
      
      good_path = true;
      Path p = null;
      try {
        p = new Path( found_paths[x] );
      } catch(java.lang.IllegalArgumentException e) {
        good_path = false;
      }
      
      if ( good_path ) {
        FileStatus f = localFs.getFileStatus(p);
        if (f.isDirectory()) {
          
          // is dir: look at all immediately relative files
          // also: we're not currently searching subdirs
          
          //System.out.println( "Is a directory: " + found_paths[x] );
         
          for (FileStatus child_fst : localFs.listStatus(f.getPath())) {
          //for (FileStatus child_fst : localFs.listFiles(f.getPath(), false)) {
          
           
            
            if (child_fst.isDirectory()) {
            
              
              System.out.println( "\tIs SubDir: " + child_fst.toString() );
              
              //String subdirlist = BuildDirList(fs, child_fst);
              
              //if (subdirlist.length() > 0 && dirList.length() > 0) {
              //  dirList.append(",");
              //}
              
              //dirList.append(subdirlist);
              
            } else {

              
              BlockLocation[] bl = localFs.getFileBlockLocations(p, 0, f.getLen());
              
              System.out.println( "\tfound blocks: " + bl.length );
      
              for (BlockLocation b : bl) {
                FileSplit split = FileSplit.newBuilder().setPath(p.toString())
                    .setOffset(b.getOffset()).setLength(b.getLength()).build();
      
                System.out.println( "\tsplit: " + split.toString() );
                
      //          StartupConfiguration config = StartupConfiguration.newBuilder()
      //              .setBatchSize(batchSize).setIterations(iterationCount)
      //              .setOther(appConfig).setSplit(split).build();
      
                String wid = "worker-" + workerId;
                ConfigurationTuple tuple = null; //new ConfigurationTuple(b.getHosts()[0], wid, config);
      
                configTuples.add(tuple);
                
              } // for
              
              
            }
            
          }
          
        } else if (f.isFile()) {
          
          //System.out.println( "Is a file: " + found_paths[x] );
          
          BlockLocation[] bl = localFs.getFileBlockLocations(p, 0, f.getLen());
          
          System.out.println( "\tfound blocks: " + bl.length );
  
          for (BlockLocation b : bl) {
            FileSplit split = FileSplit.newBuilder().setPath(p.toString())
                .setOffset(b.getOffset()).setLength(b.getLength()).build();
  
            System.out.println( "\tsplit: " + split.toString() );
            
  //          StartupConfiguration config = StartupConfiguration.newBuilder()
  //              .setBatchSize(batchSize).setIterations(iterationCount)
  //              .setOther(appConfig).setSplit(split).build();
  
            String wid = "worker-" + workerId;
            ConfigurationTuple tuple = null; //new ConfigurationTuple(b.getHosts()[0], wid, config);
  
            configTuples.add(tuple);
            workerId++;
          }
          
          
        }
        
      } // if good path
      
    } // for
    
    //System.out.println( "done.\n\n" );
    
    
    return configTuples;
  }
  
  public void checkDirs() throws IOException {
    
    
    File KBoar_InputSplitTesting_dir = new File("/tmp/KBoar_InputSplitTesting");
    
    if (!KBoar_InputSplitTesting_dir.exists()) {
      System.out.println( "Creating dir... /tmp/KBoar_InputSplitTesting" );
      KBoar_InputSplitTesting_dir.mkdir();
    }    

    File KBoar_InputSplitTesting_Alpha_dir = new File("/tmp/KBoar_InputSplitTesting/alpha");
    
    if (!KBoar_InputSplitTesting_Alpha_dir.exists()) {
      System.out.println( "Creating dir... /tmp/KBoar_InputSplitTesting/alpha" );
      KBoar_InputSplitTesting_Alpha_dir.mkdir();
    }    
    
    File KBoar_InputSplitTesting_Beta_dir = new File("/tmp/KBoar_InputSplitTesting/beta");
    
    if (!KBoar_InputSplitTesting_Beta_dir.exists()) {
      System.out.println( "Creating dir... /tmp/KBoar_InputSplitTesting/beta" );
      KBoar_InputSplitTesting_Beta_dir.mkdir();
    }    

    File KBoar_InputSplitTesting_a_txt = new File("/tmp/KBoar_InputSplitTesting/a.txt");
    
    if (!KBoar_InputSplitTesting_a_txt.exists()) {
      System.out.println( "Creating file... /tmp/KBoar_InputSplitTesting/a.txt" );
      KBoar_InputSplitTesting_a_txt.createNewFile();
    }    
    
    File KBoar_InputSplitTesting_b_txt = new File("/tmp/KBoar_InputSplitTesting/b.txt");
    
    if (!KBoar_InputSplitTesting_b_txt.exists()) {
      System.out.println( "Creating file... /tmp/KBoar_InputSplitTesting/b.txt" );
      KBoar_InputSplitTesting_b_txt.createNewFile();
    }    

    File KBoar_InputSplitTesting_alpha_c_txt = new File("/tmp/KBoar_InputSplitTesting/alpha/c.txt");
    
    if (!KBoar_InputSplitTesting_alpha_c_txt.exists()) {
      System.out.println( "Creating file... /tmp/KBoar_InputSplitTesting/alpha/c.txt" );
      KBoar_InputSplitTesting_alpha_c_txt.createNewFile();
    }    
    
    File KBoar_InputSplitTesting_alpha_gamma = new File("/tmp/KBoar_InputSplitTesting/alpha/gamma");
    
    if (!KBoar_InputSplitTesting_alpha_gamma.exists()) {
      System.out.println( "Creating dir... /tmp/KBoar_InputSplitTesting/alpha/gamma" );
      KBoar_InputSplitTesting_alpha_gamma.mkdir();
    }    
    
  }
  
  @Test 
  public void testInputPaths() throws IOException {
    
    checkDirs();
    
    //String path0 = "";
    
    //listDir( path0 );
    
    
    // single file
    String path0 = "/tmp/KBoar_InputSplitTesting/a.txt";
    HashSet<FileSplit> splits0 = getFileSplits_Proto( path0 );
    
    assertEquals( 1, splits0.size() );

    // empty dir
    String path1 = "";
    HashSet<FileSplit> splits1 = getFileSplits_Proto( path1 );

    assertEquals( 0, splits1.size() );
    
    // dir with subdirs, no files
    String path2 = "/tmp/KBoar_InputSplitTesting/beta/";
    HashSet<FileSplit> splits2 = getFileSplits_Proto( path2 );
    
    assertEquals( 0, splits2.size() );
    
    // dir with subdirs, 1 files
    String path3 = "/tmp/KBoar_InputSplitTesting/alpha/";
    HashSet<FileSplit> splits3 = getFileSplits_Proto( path3 );

    assertEquals( 1, splits3.size() );
    
    // dir with subdirs, N files
    String path4 = "/tmp/KBoar_InputSplitTesting/";
    HashSet<FileSplit> splits4 = getFileSplits_Proto( path4 );

    assertEquals( 2, splits4.size() );
    
    

    String path5 = "/tmp/KBoar_InputSplitTesting/a.txt,/tmp/KBoar_InputSplitTesting/alpha/";
    HashSet<FileSplit> splits5 = getFileSplits_Proto( path5 );
    
    assertEquals( 2, splits5.size() );
    
    // comma seperated list of file paths
    String path6 = "/tmp/KBoar_InputSplitTesting/a.txt,/tmp/KBoar_InputSplitTesting/b.txt";
    HashSet<FileSplit> splits6 = getFileSplits_Proto( path6 );
    
    assertEquals( 2, splits6.size() );

    // comma seperated list of directories
    String path7 = "/tmp/KBoar_InputSplitTesting,/tmp/KBoar_InputSplitTesting/alpha";
    HashSet<FileSplit> splits7 = getFileSplits_Proto( path7 );
    
    assertEquals( 3, splits7.size() );
    
    // mix of: comma seperated list of directories, file paths
    String path8 = "/tmp/KBoar_InputSplitTesting/a.txt,/tmp/KBoar_InputSplitTesting/b.txt,/tmp/KBoar_InputSplitTesting/alpha";
    HashSet<FileSplit> splits8 = getFileSplits_Proto( path8 );

    assertEquals( 3, splits8.size() );

    
    
    // What is the default behavior here? fail or just drop bad path?
    
    // mix of: BAD PATH, comma seperated list of directories, file paths
    String path9 = "/tmp/KBoar_InputSplitTesting/z.txt,/tmp/KBoar_InputSplitTesting/b.txt,/tmp/KBoar_InputSplitTesting/alpha";
    HashSet<FileSplit> splits9 = getFileSplits_Proto( path9 );

    assertEquals( 2, splits9.size() );
    
    
  }
/*  
  @Test
  public void testSplits() throws FileNotFoundException, IOException {
    
    
    
    //Configuration conf;
    ApplicationAttemptId appAttemptId;
    String appName = "test_app";
    Properties props;
    
    props = new Properties();
//    props.load(new FileInputStream(ConfigFields.APP_CONFIG_FILE)); // Should be
  
    props.setProperty(ConfigFields.APP_INPUT_PATH, input_file.toString());
    Map<CharSequence, CharSequence> appConfig = new HashMap<CharSequence, CharSequence>();
    
    for (Map.Entry<Object, Object> prop : props.entrySet()) {
      appConfig.put((String) prop.getKey(), (String) prop.getValue());
    }

    // setup ----------------------
    
    Path p = new Path(props.getProperty(ConfigFields.APP_INPUT_PATH));
    //FileSystem fs = FileSystem.get(conf);
    //FileStatus f = fs.getFileStatus(p);
    FileStatus f = localFs.getFileStatus(p);
    BlockLocation[] bl = localFs.getFileBlockLocations(p, 0, f.getLen());
    Set<ConfigurationTuple> configTuples = new HashSet<ConfigurationTuple>();
    int workerId = 0;

    for (BlockLocation b : bl) {
      FileSplit split = FileSplit.newBuilder().setPath(p.toString())
          .setOffset(b.getOffset()).setLength(b.getLength()).build();

      System.out.println( "split: " + split.toString() );
      
//      StartupConfiguration config = StartupConfiguration.newBuilder()
//          .setBatchSize(batchSize).setIterations(iterationCount)
//          .setOther(appConfig).setSplit(split).build();

      String wid = "worker-" + workerId;
//      ConfigurationTuple tuple = new ConfigurationTuple(b.getHosts()[0], wid,
//          config);

//      configTuples.add(tuple);
      workerId++;
    }

    //return configTuples;    
    
  }
  */
  
}

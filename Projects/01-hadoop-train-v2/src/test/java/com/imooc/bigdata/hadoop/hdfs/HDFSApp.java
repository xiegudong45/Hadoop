package com.imooc.bigdata.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/*
  Use Java Api to manipulate HDFS.
*/
public class HDFSApp {
  public static final String HDFS_PATH = "hdfs://10.0.0.226:8020";
  FileSystem fileSystem = null;
  Configuration configuration = null;

  @Before
  public void setUp() throws Exception {
    System.out.println("-------set up ------");
    configuration = new Configuration();

    // default is 3, but we set replica is 1 in hadoop-2.6.0../etc/hadoop/hdfs-site.xml
    configuration.set("dfs.replication", "1");
    /*
     * Build a HDFS system client instance
     * First parameter: URI of HDFS
     * Second parameter: Client's default properties
     * Third: Username
     */
    fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
  }

  /*
   * create a HDFS folder
   */

  @Test
  public void mkdir() throws Exception {
    fileSystem.mkdirs(new Path("/hdfsapi/test"));
  }

  /*
   * view HDFS content
   */
  @Test
  public void text() throws Exception {
    FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
    IOUtils.copyBytes(in, System.out, 1024);
  }

  /*
   * create a file in HDFS
   */
  @Test
  public void create() throws Exception {
    // FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
    FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));

    out.writeUTF("hello pk1");
    out.flush();
    out.close();
  }

  @Test
  public void testReplication() throws Exception {
    System.out.println(configuration.get("dfs.replication"));
  }

  @Test
  public void rename() throws Exception {
    Path oldPath = new Path("/hdfsapi/test/b.txt");
    Path newPath = new Path("/hdfsapi/test/c.txt");
    boolean result = fileSystem.rename(oldPath, newPath);
    System.out.println(result);

  }

  /*
   * Copy local file to HDFS
   */
  @Test
  public void copyFromLocalFile() throws Exception {
    Path src = new Path("/Users/xieshenghao/Desktop/Hello/HELP.md");
    Path dst = new Path("/hdfsapi/test/");
    fileSystem.copyFromLocalFile(src, dst);
  }

  /*
   * Copy local big file to HDFS with loading bar
   */
  @Test
  public void copyFromLocalBigFile() throws Exception {
    String inputPath = "/Users/xieshenghao/Desktop/jdk-8u221-linux-i586.tar.gz";
    InputStream in = new BufferedInputStream(new FileInputStream(new File(inputPath)));

    FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/jdk.tgz"),
            new Progressable() {
              @Override
              public void progress() {
                System.out.print(".");
              }
            });
    IOUtils.copyBytes(in, out, 4096);

  }

  /*
   * Copy HDFS file to local: download
   */
  @Test
  public void copyToLocalFile() throws Exception {
    Path src = new Path("/hdfsapi/test/a.txt" );
    Path dst = new Path("/Users/xieshenghao/Desktop/");
    fileSystem.copyToLocalFile(src, dst);
  }

  /**
   * browse all files under target folder
   */
  @Test
  public void listFile() throws Exception {
    FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
    for (FileStatus file : statuses) {
      String isDir = file.isDirectory() ? "folder" : "file";
      String permission = file.getPermission().toString();
      short replication = file.getReplication();
      long length = file.getLen();
      String path = file.getPath().toString();
      System.out.println(isDir + "\t" + permission + "\t" + replication +
              "\t" + length + "\t" + path);
    }
  }

  /**
   * Recursively find all files in the folder
   */
  @Test
  public void listFilesRecursively() throws Exception {
    RemoteIterator<LocatedFileStatus>  files = fileSystem.listFiles(new Path("/hdfsapi/test"), true);

    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      String isDir = file.isDirectory() ? "folder" : "file";
      String permission = file.getPermission().toString();
      short replication = file.getReplication();
      long length = file.getLen();
      String path = file.getPath().toString();
      System.out.println(isDir + "\t" + permission + "\t" + replication +
              "\t" + length + "\t" + path);
    }
  }

  /**
   * get blockID
   */
  @Test
  public void getFileBlockLocations() throws Exception {
    FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tgz"));
    BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    for (BlockLocation block : blocks) {
      for (String name : block.getNames()) {
        System.out.println(name + " : " + block.getOffset() + " : " +  block.getLength() + " : " + block.getHosts().toString());
      }
    }
  }

  /**
   * Delete file
   */
  @Test
  public void delete() throws Exception {
    boolean result = fileSystem.delete(new Path("/hdfsapi/test/jdk.tgz"), true);
    System.out.println(result);
  }

  @After
  public void tearDown() {
    configuration = null;
    fileSystem = null;
    System.out.println("-------tear down------");
  }


}

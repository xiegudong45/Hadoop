package com.imooc.bigdata.hadoop.hdfs;

/*
 * Use HDFS API to do word count.
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HDFSWCApp01 {
  public static void main(String[] args) throws Exception {
    // Read files in HDFS
    Path input = new Path("/hdfsapi/test/hello.txt");

    // Get HDFS
    FileSystem fs = FileSystem.get(new URI("hdfs://10.0.0.226:8020/"), new Configuration(), "hadoop");

    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

    ImoocMapper mapper = new WordCountMapper();
    ImoocContext context = new ImoocContext();

    while (iterator.hasNext()) {
      LocatedFileStatus file = iterator.next();
      FSDataInputStream in = fs.open(file.getPath());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));

      String line = reader.readLine();;
      while (line != null) {

        // count word frequency (hello, 3)
        // save word and word count to cache
        mapper.map(line, context);
        line = reader.readLine();
      }
      reader.close();
      in.close();
    }
    // Store results in cache

    Map<Object, Object> contextMap = context.getCacheMap();


    Path output = new Path("/hdfsapi/output/");
    FSDataOutputStream out = fs.create(new Path(output, new Path("wc.out")));

    // store results to out
    Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
    for (Map.Entry<Object, Object> entry : entries) {
      out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
    }

    out.close();
    fs.close();

    System.out.println("Finished");
  }

}

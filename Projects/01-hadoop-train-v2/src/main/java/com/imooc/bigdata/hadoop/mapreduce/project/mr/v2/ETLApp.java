package com.imooc.bigdata.hadoop.mapreduce.project.mr.v2;

import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.ContentUtils;
import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.LogParser;
import com.imooc.bigdata.hadoop.mapreduce.project.mr.v1.PageStatApp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLApp {

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();

    FileSystem fileSystem = FileSystem.get(configuration);
    // Path outputPath = new Path("input/etl");
    Path outputPath = new Path(args[1]);
    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }

    Job job = Job.getInstance(configuration);
    job.setJarByClass(ETLApp.class);

    job.setMapperClass(MyMapper.class);


    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);



    // FileInputFormat.setInputPaths(job, new Path("input/raw/trackinfo_20130721" +
    //         ".data"));
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    // FileOutputFormat.setOutputPath(job, new Path("input/etl"));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
  }

  static class MyMapper extends Mapper<LongWritable, Text, NullWritable,
          Text> {
    private LongWritable ONE = new LongWritable(1);

    private LogParser logParser;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
      logParser = new LogParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String log = value.toString();
      Map<String, String> infos = logParser.parse(log);
      String ip = infos.get("ip");
      String country = infos.get("country");
      String province = infos.get("province");
      String city = infos.get("city");
      String url = infos.get("url");
      String time = infos.get("time");
      String pageId = ContentUtils.getPageId(url);

      StringBuilder builder = new StringBuilder();
      builder.append(ip).append("\t");
      builder.append(country).append("\t");
      builder.append(province).append("\t");
      builder.append(city).append("\t");
      builder.append(url).append("\t");
      builder.append(time).append("\t");
      builder.append(pageId).append("\t");

      context.write(NullWritable.get(), new Text(builder.toString()));
    }
  }
}

package com.imooc.bigdata.hadoop.mapreduce.project.mr.v2;


import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.ContentUtils;
import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.GetPageId;
import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStatV2App {
  public static void main(String[] args) throws Exception{
    Configuration configuration = new Configuration();

    FileSystem fileSystem = FileSystem.get(configuration);
    // Path outputPath = new Path("output/v2/pagestat");
    Path outputPath = new Path(args[1]);

    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true);
    }

    Job job = Job.getInstance(configuration);
    job.setJarByClass(PageStatV2App.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // FileInputFormat.setInputPaths(job, new Path("input/etl"));
    // FileOutputFormat.setOutputPath(job, new Path("output/v2/pagestat"));

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  static class MyMapper extends Mapper<LongWritable, Text, Text,
          LongWritable> {
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
      Map<String, String> infos = logParser.parseV2(log);
      if(StringUtils.isNotBlank(infos.get("url"))){
        String pageId = GetPageId.getPageId(infos.get("url"));
        if (StringUtils.isNotBlank(pageId)) {
          context.write(new Text(pageId), ONE);
        } else {
          context.write(new Text("-"), ONE);
        }

      } else {
        context.write(new Text("-"), ONE);
      }
    }
  }

  static class MyReducer extends Reducer<Text, LongWritable, Text,
          LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
                          Context context) throws IOException,
            InterruptedException {
      long count = 0;
      for (LongWritable value : values) {
        count++;
      }

      context.write(key, new LongWritable(count));
    }
  }
}

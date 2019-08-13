package com.imooc.bigdata.hadoop.mapreduce.access;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessLocalApp {
  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();

    Job job = Job.getInstance(configuration);
    job.setJarByClass(AccessLocalApp.class);

    job.setMapperClass(AccessMapper.class);
    job.setReducerClass(AccessReducer.class);

    // 设置自定义分区规则
    job.setPartitionerClass(AccessPartitioner.class);
    // 设置reduce个数(几个分区)
    job.setNumReduceTasks(3);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Access.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputKeyClass(Access.class);

    FileInputFormat.setInputPaths(job, new Path("access/input"));
    FileOutputFormat.setOutputPath(job, new Path("access/output"));

    job.waitForCompletion(true);
  }
}

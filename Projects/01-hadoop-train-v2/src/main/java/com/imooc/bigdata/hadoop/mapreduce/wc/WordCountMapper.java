package com.imooc.bigdata.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN: Map任务读数据的key类型， offset， 是每行数据起始位置的偏移量，long
 * VALUEIN: Map任务读数据的value类型，起始是一行行的String
 *
 * KEYOUT: map方法自定义实现输出的key的类型
 * VALUEOUT：map方法自定义实现输出的value的类型， Integer
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // split each row according to separator
    String[] words = value.toString().split("\t");
    for (String word : words) {
      context.write(new Text(word.toLowerCase()), new IntWritable(1));
    }
  }

}

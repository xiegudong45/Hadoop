package com.imooc.bigdata.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text,
        IntWritable> {
  /**
   * (hello, 1) (world, 1)
   * (hello, 1) (world, 1)
   * (hello, 1) (world, 1)
   * (welcome, 1)
   *
   * map 的输出到reduce端，是按照相同的key分发到一个reduce上去执行
   */


  @Override
  protected void reduce(Text key, Iterable<IntWritable> values,
                        Context context) throws IOException,
          InterruptedException {
    int count = 0;
    Iterator<IntWritable> iterator = values.iterator();
    while(iterator.hasNext()) {
      IntWritable value = iterator.next();
      count += value.get();
    }

    context.write(key, new IntWritable(count));
  }
}

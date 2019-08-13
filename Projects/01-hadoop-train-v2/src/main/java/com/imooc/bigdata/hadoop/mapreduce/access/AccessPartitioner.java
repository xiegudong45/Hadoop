package com.imooc.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartitioner extends Partitioner<Text, Access> {

  /**
   *
   * @param text 手机号
   * @param access
   * @param i reducer的数量
   * @return
   */
  @Override
  public int getPartition(Text phone, Access access, int nunReduceTasks) {
    if(phone.toString().startsWith("13")) {
      return 0;
    } else if (phone.toString().startsWith("15")) {
      return 1;
    } else {
      return 2;
    }
  }
}

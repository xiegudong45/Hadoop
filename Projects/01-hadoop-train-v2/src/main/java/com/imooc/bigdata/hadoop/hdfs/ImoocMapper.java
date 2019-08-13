package com.imooc.bigdata.hadoop.hdfs;

public interface ImoocMapper {

  /**
   * Read every line from input
   * @param line input data
   * @param context cache
   */
  public void map(String line, ImoocContext context);


}

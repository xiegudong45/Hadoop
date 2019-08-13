package com.imooc.bigdata.hadoop.mr.project.utils;

import com.imooc.bigdata.hadoop.mapreduce.project.mr.utils.IPParser;
import org.junit.Test;

public class IPTest {
  @Test
  public void testIP() {
    IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("124.79" +
            ".172.232");
    System.out.println(regionInfo.getCountry());
    System.out.println(regionInfo.getProvince());
    System.out.println(regionInfo.getCity());
  }
}

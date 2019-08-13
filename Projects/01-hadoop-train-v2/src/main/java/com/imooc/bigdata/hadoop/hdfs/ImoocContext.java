package com.imooc.bigdata.hadoop.hdfs;

/*
 * Customized context
 */

import java.util.HashMap;
import java.util.Map;

public class ImoocContext {
  private Map<Object, Object> cacheMap = new HashMap<>();

  public Map<Object, Object> getCacheMap() {
    return cacheMap;
  }

  /**
   * write data to cache
   * @param key: word
   * @param val: word count
   */
  public void write(Object key, Object val) {
    cacheMap.put(key, val);
  }

  /**
   * Get word count from cache
   * @param key word
   * @return word count
   */
  public Object get(Object key) {
    return cacheMap.get(key);
  }
}

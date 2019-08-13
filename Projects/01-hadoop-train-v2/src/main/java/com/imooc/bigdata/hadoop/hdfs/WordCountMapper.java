package com.imooc.bigdata.hadoop.hdfs;


/**
 * Customized word count class
 */
public class WordCountMapper implements ImoocMapper{
  public void map(String line, ImoocContext context) {
    String[] words = line.split("\t");

    for (String word : words) {
      Object value = context.get(word);
      if (value == null) { // if there's no word in cache
        context.write(word, 1);
      } else {
        int v = Integer.parseInt(value.toString());
        context.write(word, v + 1);
      }
    }

  }
}

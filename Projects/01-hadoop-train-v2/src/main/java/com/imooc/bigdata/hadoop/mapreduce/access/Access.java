package com.imooc.bigdata.hadoop.mapreduce.access;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂数据类型
 * 1) 按照Hadoop的规范，需要实现 Writable
 * 2) 按照Hadoop的规范，需要实现两个方法(write & readFields)
 * 3) 需要定义默认的constructor
 */
public class Access implements Writable {
  private String phone;
  private long down;
  private long up;
  private long sum;

  public Access() {}

  public Access(String phone, long down, long up) {
    this.phone = phone;
    this.down = down;
    this.up = up;
    this.sum = up + down;
  }

  public String getPhone() {
    return phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public long getDown() {
    return down;
  }

  public void setDown(long down) {
    this.down = down;
  }

  public long getUp() {
    return up;
  }

  public void setUp(long up) {
    this.up = up;
  }

  public long getSum() {
    return sum;
  }

  public void setSum(long sum) {
    this.sum = sum;
  }

  @Override
  public String toString() {
    return phone +
            ", " + down +
            ", " + up +
            ", " + sum;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(phone);
    out.writeLong(up);
    out.writeLong(down);
    out.writeLong(sum);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.phone = in.readUTF();
    this.up = in.readLong();
    this.down = in.readLong();
    this.sum = in.readLong();
  }
}

package org.apache.hadoop.hbase.index.interpreter;

public class ByteRange {
  private byte[] bytes;
  private int offset;
  private int length;
  
  public byte[] getBytes() {
    return bytes;
  }
  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }
  public int getOffset() {
    return offset;
  }
  public void setOffset(int offset) {
    this.offset = offset;
  }
  public int getLength() {
    return length;
  }
  public void setLength(int length) {
    this.length = length;
  }
  
  
}

package org.apache.hadoop.hbase.index.interpreter;


public interface DataInterpreter {
  public ByteRange getInterpretedValue(ByteRange range);
}

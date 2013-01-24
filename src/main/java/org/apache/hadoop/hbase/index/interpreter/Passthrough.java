package org.apache.hadoop.hbase.index.interpreter;

public class Passthrough implements DataInterpreter{

  public ByteRange getInterpretedValue(ByteRange range) {
    return range;
  }
  
}

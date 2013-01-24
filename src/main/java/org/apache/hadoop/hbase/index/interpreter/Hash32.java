package org.apache.hadoop.hbase.index.interpreter;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.hash.Hash;

public class Hash32 implements DataInterpreter{

  private static Hash HASH = Hash.getInstance(Hash.MURMUR_HASH);
  
  public ByteRange getInterpretedValue(ByteRange range) {
    ByteRange newRange = new ByteRange();
    int hash = HASH.hash(range.getBytes(), range.getOffset(), range.getLength());
    newRange.setLength(4);
    newRange.setOffset(0);
    newRange.setBytes(Bytes.toBytes(hash));
    return newRange;
  }

  
}

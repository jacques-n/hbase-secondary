package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

public class CheckValue {
  private byte[] rowKey;
  private byte[] checkColumnFamily;
  private byte[] checkColumnQualifier;
  private byte[] checkValue;
  
  public boolean check(HRegion region) throws IOException{
    Get g = new Get(rowKey);
    g.addColumn(checkColumnFamily, checkColumnQualifier);
    Result r = region.get(g, null);
    if(r.isEmpty()) return false;
    return Arrays.equals(checkValue, r.getValue(checkColumnFamily, checkColumnQualifier));
  }
  
  public byte[] getRowKey(){
    return rowKey;
  }
}

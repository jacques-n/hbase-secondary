package org.apache.hadoop.hbase.index;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

public class IndexingCoprocessor implements Coprocessor{

  public void start(CoprocessorEnvironment env) throws IOException {
    HTableInterface meta = env.getTable(HConstants.META_TABLE_NAME);
    HTableDescriptor descriptor = meta.getTableDescriptor();
    meta.
  }

  public void stop(CoprocessorEnvironment env) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  
  

}

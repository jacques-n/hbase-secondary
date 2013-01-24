package org.apache.hadoop.hbase.index.client;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.index.common.OptionsHelper;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;

public class PerTablePool {

  private volatile TableIndexOptions indexOptions;
  private String nameAsString;
  private byte[] nameAsBytes;
  
  public PerTablePool
  
  private void refreshIndexOptionSet(){
    HTableDescriptor desc = htable.getTableDescriptor();
    TableIndexOptions newOptions = OptionsHelper.getOptionSet(desc);
    if(indexOptions.equals(newOptions)) return;
  }
  
  public HTableInterface getTable(){
    return null;
  }
}

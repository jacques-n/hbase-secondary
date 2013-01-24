package org.apache.hadoop.hbase.index.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;

import com.google.common.base.Preconditions;

public class IndexHTableFactory implements HTableInterfaceFactory {

  private IndexHTablePool pool;

  public HTableInterface createHTableInterface(Configuration config,
      byte[] tableName) {
    
    // make sure pool has been set.
    Preconditions.checkNotNull( pool,
            "You must inform the IndexHTableFactory of the HTable pool you're using.  "
                + " Otherwise, it is unable to create secondary HTable connections as necessary.");
    try {
      return new IndexHTable(config, tableName, pool);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public void setPool(IndexHTablePool pool){
    this.pool = pool;
  }

  public void releaseHTableInterface(HTableInterface table) throws IOException {
    table.close();
  }
  
}

package org.apache.hadoop.hbase.index.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.index.IndexingRegionObserver;
import org.apache.hadoop.hbase.index.common.IndexingConstants;
import org.apache.hadoop.hbase.index.common.OptionsHelper;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;

/**
 * This is an alternative implementation of HTablePool that automatically
 * leverages indexes. This is the public interface into indexes because sparse
 * and unique indexes are realized as separate tables. As such, a single get or
 * scan will potentially operate across multiple HTable connections. To leverage
 * a single set of pools, index interactions operate through the HTablePool
 * abstraction rather than a single HTableInterface.
 */
public class IndexHTablePool extends HTablePool {

  private static final Log LOG = LogFactory.getLog(IndexHTablePool.class);
  
  private final Object closeTableLock = new Object();
  private final ConcurrentHashMap<String, TableIndexOptions> optionMap;
  private final TableDescriptorMaintainer maintainerThread;

  private class TableDescriptorMaintainer extends Thread {
    
    final long sleepTime;

    public TableDescriptorMaintainer(long sleepTime) {
      this.sleepTime = sleepTime;
      this.setDaemon(true);
      this.setName("Index Table Descriptor Updating Thread");
    }

    @Override
    public void run() {
      try {

        while (true) {
          Thread.sleep(sleepTime);
          for (Map.Entry<String, TableIndexOptions> options : optionMap
              .entrySet()) {

          }
        }
      } catch (InterruptedException e){
        LOG.info("Interrupted while running table descriptor maintainer.", e);
        return;
      } catch (Exception e) {

      }
    }

  }

  private IndexHTablePool(Configuration config, int maxSize,
      HTableInterfaceFactory tableFactory, PoolType poolType) {
    super(config, maxSize, tableFactory, poolType);
    optionMap = new ConcurrentHashMap<String, TableIndexOptions>(10, .75f, 8);
    maintainerThread = new TableDescriptorMaintainer(IndexingConstants.TABLE_DESCRIPTOR_UPDATE_FREQUENCY_MS);
    maintainerThread.start();

  }

  @Override
  public void closeTablePool(String tableName) throws IOException {
    synchronized (closeTableLock) {
      super.closeTablePool(tableName);
    }

  }

  @Override
  protected HTableInterface createHTable(String tableName) {
    HTableInterface table = super.createHTable(tableName);
    
    // we don't need synchronization here since optionMap is concurrent and overwrites don't matter.
    if(!optionMap.containsKey(tableName)){
      optionMap.put(tableName, OptionsHelper.getOptionSet(table.getTableDescriptor()));
    }
    
    return table; 
  }

  public static IndexHTablePool getPool() {
    return getPool(HBaseConfiguration.create(), Integer.MAX_VALUE,
        PoolType.Reusable,
        IndexingConstants.TABLE_DESCRIPTOR_UPDATE_FREQUENCY_MS);
  }

  public static IndexHTablePool getPool(Configuration config, int maxSize,
      PoolType poolType, long tableDescriptorFrequencyInMS) {
    IndexHTableFactory factory = new IndexHTableFactory();
    IndexHTablePool pool = new IndexHTablePool(config, maxSize, factory,
        poolType);
    factory.setPool(pool);
    return pool;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    super.close();
  }

}

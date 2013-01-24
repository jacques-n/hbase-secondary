package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.common.OptionsHelper;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.protobuf.InvalidProtocolBufferException;

public class IndexingRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(IndexingRegionObserver.class);

  
  private IndexImplementation indexer;  // no need to volatile since indexingEnabled will need to be reset which forces barrier.
  private volatile boolean indexingEnabled = false;

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    
    HTableDescriptor d = e.getEnvironment().getRegion().getTableDesc();
    try{
      TableIndexOptions options = OptionsHelper.getOptionSet(d);
      if(options.equals(TableIndexOptions.getDefaultInstance())){
        LOG.debug("Table " + d.getNameAsString() + " currently has no configured indexes.");
      }else{
        indexer = IndexImplementation.newImplementation(options);
        indexingEnabled = true;

      }
    } catch (InvalidProtocolBufferException ex) {
      LOG.error("Failure while loading TableIndexOptions for table " + d.getNameAsString(), ex);
    }
    

    
  }


  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
    if(!indexingEnabled) return super.preScannerOpen(e, scan, s);

    
    if(s != null) throw new IOException("A predefined regionscanner spells index troubles.. probably another coprocessor fighting with this one.");
    
    e.bypass();
    return indexer.getIndexedScanner(e, scan);
  }

  
  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
    if(!indexingEnabled){
      super.prePut(e, put, edit, writeToWAL);
      return;
    }
    MutationSet ms = MutationSet.newSet(e, put, writeToWAL);
    ms = indexer.applyIndexMutation(ms);
    long now = EnvironmentEdgeManager.currentTimeMillis();
    ms.execute(e, edit, now);
    e.bypass();
    return;
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {

    if(!indexingEnabled){
      super.preDelete(e, delete, edit, writeToWAL);
      return;
    }

    MutationSet ms = MutationSet.newSet(e, delete, writeToWAL);
    ms = indexer.applyIndexMutation(ms);
    
    ms.execute(e, edit);
    e.bypass();
    return;
  }


  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, HStore store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException {
    if(!indexingEnabled) return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
    c.bypass();
    return indexer.getCompactionScanner(c, store, scanners, scanType, earliestPutTs, s);

  }


  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<KeyValue> results) throws IOException {
    
    // the get has a row key, we'll skip over indexing functionality.  only if a row key is empty will we switch to index usage.
    if(!indexingEnabled || get.getRow().length > 0){
      super.preGet(e, get, results);     
      return;
    }

    
    
    Scan s = new Scan(
    if()
    get.setFilter(indexer.addIndexFilters(get.getFilter()));
    
    
    
  }
  

  
   

  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists)
      throws IOException {
    
    if(!indexingEnabled) return super.preExists(e, get, exists);
    
    return false;
  }


  @Override
  public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
      Put put, boolean result) throws IOException {

    if(!indexingEnabled) return super.preCheckAndPut(e, row, family, qualifier, compareOp, comparator, put, result);
    
    return false;
  }


  @Override
  public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
    
    if(!indexingEnabled) return super.preCheckAndDelete(e, row, family, qualifier, compareOp, comparator, delete, result);
    
    return false;
  }


  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append)
      throws IOException {
    if(!indexingEnabled) return super.preAppend(e, append);
    
    return null;
  }



  
  

}
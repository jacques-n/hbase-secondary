package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RowProcessor;
import org.apache.hadoop.hbase.regionserver.ScanType;

public abstract class IndexImplementation implements MutationTransformer{

  
  
  public static IndexImplementation newImplementation(TableIndexOptions options){
    return null;
  }
  
  /**
   * Responsible for supplementing any CritieraFilters with appropriate scanner
   * filters.
   */
  public abstract RegionScanner getIndexedScanner(ObserverContext<RegionCoprocessorEnvironment> e,  Scan scan) throws IOException;
  public abstract void addMutation(Mutation mutation, MutationSet mutationSet);
  public abstract RowProcessor<Boolean> getIndexedMutations(MutationSet mutations) throws IOException;
  public abstract InternalScanner getCompactionScanner(ObserverContext<RegionCoprocessorEnvironment> c, HStore store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException;
  public abstract Filter addIndexFilters(Filter f);
  public abstract MutationFilter getMutationFilter();

  

}

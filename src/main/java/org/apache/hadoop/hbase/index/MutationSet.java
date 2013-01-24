package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionAccessor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class MutationSet{

  private Collection<CheckValue> checkValues = new LinkedList<CheckValue>();
  private List<Mutation> mutations = new LinkedList<Mutation>();

  boolean doChecks(HRegion region) throws IOException {
    for (CheckValue check : checkValues) {
      if (!check.check(region)) {
        return false;
      }
    }
    return true;
  }
  
  Collection<CheckValue> getCheckValues(){
    return checkValues;
  }
  
  List<Mutation> getMutations(){
    return mutations;
  }
  
  
  public static MutationSet newSet(
      ObserverContext<RegionCoprocessorEnvironment> environment, Put p,
      boolean writeToWal) {
    MutationSet s = new MutationSet(environment);
    s.lockRows.add(p.getRow());
    s.mutations.add(p);
    return s;
  }

  public static MutationSet newSet(
      ObserverContext<RegionCoprocessorEnvironment> environment, Delete d,
      boolean writeToWal) {
    MutationSet s = new MutationSet(environment);
    s.lockRows.add(d.getRow());
    s.mutations.add(d);
    return s;
  }
}

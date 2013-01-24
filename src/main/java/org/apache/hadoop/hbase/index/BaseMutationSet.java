package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
import org.apache.hadoop.hbase.index.MutationSet.RowProc;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionAccessor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class BaseMutationSet {

  
  private ObserverContext<RegionCoprocessorEnvironment> env;
  private Collection<byte[]> lockRows = new LinkedList<byte[]>();
  private IndexImplementation indexImplementation;
  private Collection<CheckValue> checkValues = new LinkedList<CheckValue>();
  private List<Mutation> mutations = new LinkedList<Mutation>();

  private final RowProc proc = new RowProc();

  private MutationSet(ObserverContext<RegionCoprocessorEnvironment> environment, IndexImplementation indexImplementation) {
    this.env = environment;
  }

  private boolean doChecks(HRegion region) throws IOException {
    for (CheckValue check : checkValues) {
      if (!check.check(region)) {
        return false;
      }
    }
    return true;
  }

  private boolean execute(HRegion region,
      List<KeyValue> outoingMutations /* return */,
      WALEdit walEdit /* return */, long now) throws IOException {

    if (!doChecks(region)){
      return false;
    }
    
    byte[] byteNow = Bytes.toBytes(now);
    // Check mutations and apply edits to a single WALEdit

    ListIterator<Mutation> iter = mutations.listIterator();

    while (iter.hasNext()) {
      Mutation m = iter.next();
      MutationAcceptor acceptor;
      
      // convert appends to puts.
      if (m instanceof Append){
        m = morphAppendToPut((Append) m, region, now);
        if(acceptor == null) acceptor = new ListAcceptor(iter); 
        indexImplementation.
      }
      if (m instanceof Put) {
        Map<byte[], List<KeyValue>> familyMap = m.getFamilyMap();
        HRegionAccessor.checkFamilies(familyMap.keySet(), region);
        HRegionAccessor.checkTimestamps(familyMap, now, region);
        HRegionAccessor.updateKVTimestamps(familyMap.values(), byteNow, region);
      } else if (m instanceof Delete) {
        Delete d = (Delete) m;
        HRegionAccessor.prepareDelete(d, region);
        HRegionAccessor.prepareDeleteTimestamps(d.getFamilyMap(), byteNow,
            region);
      } else {
        throw new DoNotRetryIOException(
            "Action must be Put or Delete. But was: " + m.getClass().getName());
      }

      for (List<KeyValue> edits : m.getFamilyMap().values()) {
        boolean writeToWAL = m.getWriteToWAL();
        for (KeyValue kv : edits) {
          outoingMutations.add(kv);
          if (writeToWAL) {
            walEdit.add(kv);
          }
        }
      }

    }

    return true;

  }

  protected void preExecute() {

  }

  protected void postExecute(boolean success) {

  }


  public void addMutation(Mutation mutation) {
    mutations.add(mutation);
  }

  public void addMutations(Collection<Mutation> mutations) {
    this.mutations.addAll(mutations);
  }

  public void setCheckValue(byte[] columnFamilyName,
      byte[] columnQualifierName, byte[] cellValue) {

  }

  // private Collection<byte[]> getOrderedRowKeys(Collection<Mutation>
  // mutations) {
  // HashSet<byte[]> rowsToLockHash = new HashSet<byte[]>();
  //
  // for (Mutation m : mutations) {
  // rowsToLockHash.add(m.getRow());
  // }
  // ArrayList<byte[]> rowsToLock = new ArrayList<byte[]>(rowsToLockHash);
  // Collections.sort(rowsToLock, Bytes.BYTES_COMPARATOR);
  //
  // return rowsToLock;
  // }

  private class RowProc extends BaseRowProcessor<Boolean> {



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

  
  
  public boolean applyMutations(){
    
  }
}

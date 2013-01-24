package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionAccessor;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

public abstract class BaseIndexRowProcessor extends BaseRowProcessor<Boolean>{

  private MutationSet mutationSet;
  private boolean success = false;

  
  private boolean doChecks(HRegion region) throws IOException {
    for (CheckValue check : mutationSet.getCheckValues()) {
      if (!check.check(region)) {
        return false;
      }
    }
    return true;
  }

  protected abstract void addAddionalPutIndexMutation(Put p, ListIterator<Mutation> iter);
    
  public List<byte[]> getRowsToLock() {
    HashSet<byte[]> rows = new HashSet<byte[]>();
    
    for(CheckValue cv : mutationSet.getCheckValues()){
      rows.add(cv.getRowKey());
    }
    
    for(Mutation m : mutationSet.getMutations()){
      rows.add(m.getRow());
    }
    
    List<byte[]> rowList = new ArrayList<byte[]>(rows);
    Collections.sort(rowList, Bytes.BYTES_COMPARATOR);
    return rowList;
  }
  

  public boolean readOnly() {
    return false;
  }



  private boolean processWithOutcome(long now, HRegion region,
      List<KeyValue> outoingMutations /* return */,
      WALEdit walEdit /* return */) throws IOException {

    if (!doChecks(region)){
      return false;
    }
    
    
    byte[] byteNow = Bytes.toBytes(now);
    // Check mutations and apply edits to a single WALEdit

    ListIterator<Mutation> iter = mutationSet.getMutations().listIterator();

    while (iter.hasNext()) {
      Mutation m = iter.next();
      
      // convert appends to puts.
      if (m instanceof Append){
        Put p = morphAppendToPut((Append) m, region, now);
        addAddionalPutIndexMutation(p, iter);
        m = p;
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

  
  private Put morphAppendToPut(Append append, HRegion region, long now)
      throws IOException {
    byte[] row = append.getRow();
    Get get = new Get(row);
    long timeStamp = append.getTimeStamp();
    Set<Entry<byte[], List<KeyValue>>> appendEntrySet = append.getFamilyMap()
        .entrySet();

    // build a get to get all the existing values for the append.
    for (Map.Entry<byte[], List<KeyValue>> e : appendEntrySet) {
      // byte[] family = e.getKey(); //not needed.
      for (KeyValue kv : e.getValue()) {
        get.addColumn(kv.getFamily(), kv.getQualifier());
      }
    }
    Result result = region.get(get, null);

    Put put = new Put(row);

    // go through the append values again and add puts of everything, leveraging
    // existing values where they were retrieved.
    for (Map.Entry<byte[], List<KeyValue>> e : appendEntrySet) {
      // byte[] family = e.getKey(); //not needed.

      for (KeyValue appendKV : e.getValue()) {
        byte[] fam = appendKV.getFamily();
        byte[] qual = appendKV.getQualifier();

        List<KeyValue> oldKVs = result.getColumn(fam, qual);
        if (oldKVs != null && !oldKVs.isEmpty()) {
          // append an existing found value.
          KeyValue oldKV = oldKVs.get(0);
          KeyValue newKV = new KeyValue(row.length, fam.length, qual.length,
              timeStamp, Type.Put, oldKV.getValueLength()
                  + appendKV.getValueLength());
          System.arraycopy(row, 0, newKV.getBuffer(), newKV.getRowOffset(),
              row.length);
          System.arraycopy(fam, 0, newKV.getBuffer(), newKV.getFamilyOffset(),
              fam.length);
          System.arraycopy(qual, 0, newKV.getBuffer(),
              newKV.getQualifierOffset(), qual.length);
          System.arraycopy(oldKV, oldKV.getValueOffset(), newKV.getBuffer(),
              newKV.getValueOffset(), oldKV.getValueLength());
          System.arraycopy(appendKV, appendKV.getValueOffset(),
              newKV.getBuffer(),
              newKV.getValueOffset() + oldKV.getValueLength(),
              appendKV.getValueLength());
          put.add(newKV);
        } else {
          // simple put.
          KeyValue newKV = new KeyValue(row.length, fam.length, qual.length,
              timeStamp, Type.Put, appendKV.getValueLength());
          System.arraycopy(row, 0, newKV.getBuffer(), newKV.getRowOffset(),
              row.length);
          System.arraycopy(fam, 0, newKV.getBuffer(), newKV.getFamilyOffset(),
              fam.length);
          System.arraycopy(qual, 0, newKV.getBuffer(),
              newKV.getQualifierOffset(), qual.length);
          System.arraycopy(appendKV, appendKV.getValueOffset(),
              newKV.getBuffer(), newKV.getValueOffset(),
              appendKV.getValueLength());
          put.add(newKV);
        }

      }
    }

    return put;
  }
  
  
  
  @Override
  public final void postProcess(HRegion region, WALEdit walEdit) throws IOException {
    postProcess(region, walEdit, success);
  }
  
  public void process(long now, HRegion region, List<KeyValue> mutations,
      WALEdit walEdit) throws IOException {
    success = processWithOutcome(now, region, mutations, walEdit);
  }
  

  public void postProcess(HRegion region, WALEdit walEdit, boolean success){
    
  }

  @Override
  public void preProcess(HRegion region, WALEdit walEdit) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      for (Mutation m : mutations) {
        if (m instanceof Put) {
          if (coprocessorHost.prePut((Put) m, walEdit, m.getWriteToWAL())) {
            // by pass everything
            return;
          }
          
        } else if (m instanceof Delete) {
          Delete d = (Delete) m;
          region.prepareDelete(d);
          if (coprocessorHost.preDelete(d, walEdit, d.getWriteToWAL())) {
            // by pass everything
            return;
          }
        }
      }
    }
  }

  @Override
  public void postProcess(HRegion region, WALEdit walEdit) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      for (Mutation m : mutations) {
        if (m instanceof Put) {
          coprocessorHost.postPut((Put) m, walEdit, m.getWriteToWAL());
        } else if (m instanceof Delete) {
          coprocessorHost.postDelete((Delete) m, walEdit, m.getWriteToWAL());
        }
      }
    }
  }
  

}


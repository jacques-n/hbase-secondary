package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.FailedSanityCheckException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

/** 
 * Class that makes package private stuff available.
 */
public class HRegionAccessor {

  public static void checkFamilies(Collection<byte[]> families, HRegion region)
      throws NoSuchColumnFamilyException {
    region.checkFamilies(families);
  }

  public static void updateKVTimestamps(
      final Iterable<List<KeyValue>> keyLists, final byte[] now, HRegion region) {
    region.updateKVTimestamps(keyLists, now);
  }

  public static void checkTimestamps(
      final Map<byte[], List<KeyValue>> familyMap, long now, HRegion region)
      throws FailedSanityCheckException {
    region.checkTimestamps(familyMap, now);
  }

  public static void prepareDelete(Delete delete, HRegion region)
      throws IOException {
    region.prepareDelete(delete);
  }

  public static void prepareDeleteTimestamps(
      Map<byte[], List<KeyValue>> familyMap, byte[] byteNow, HRegion region)
      throws IOException {
    region.prepareDeleteTimestamps(familyMap, byteNow);
  }

  
  
}

package org.apache.hadoop.hbase.index.server.scan;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Responsible for determining where we are in scan order.
 *
 */
public interface KeyMatcher {

  public int compare(byte[] keyBuffer, int offset, int length, KeyValue keyValue);
  //public boolean includeKeyValue(KeyValue incomingValue, KeyValue indexValue);
  
}

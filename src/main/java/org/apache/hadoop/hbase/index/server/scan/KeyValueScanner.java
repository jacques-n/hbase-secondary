package org.apache.hadoop.hbase.index.server.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;


/** 
 * Abstraction used to make {@link RegionScanner}s and {@link ResultScanner}s work with the same interface for index retrieval purposes.
 *
 */
public abstract class KeyValueScanner {

  /**
   * Returns the next set of key values for the selected scanner type.
   * @param keyValues The holder where the undelying implementation will insert additional key values.
   * @param limit The number of additional rows to request.
   * @return True if there are more values available.  False is the underlying scanner has no more values remaining.
   */
  public abstract boolean next(List<KeyValue> keyValues, int limit) throws IOException;
}

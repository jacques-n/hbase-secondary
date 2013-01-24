package org.apache.hadoop.hbase.index.server.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class InternalKeyValueScanner extends KeyValueScanner{

  private final RegionScanner scanner;

  /**
   * Construct a new InternalKeyValueScanner based on a RegionScanner.  Note that this scanner does not do any keyValue filtering.
   * @param scanner The RegionScanner to proxy.
   */
  public InternalKeyValueScanner(RegionScanner scanner) {
    super();
    this.scanner = scanner;
  }

  @Override
  public boolean next(List<KeyValue> keyValues, int limit) throws IOException {
    return scanner.next(keyValues, limit);
  }

}

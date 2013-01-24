package org.apache.hadoop.hbase.index.server.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class ExternalKeyValueScanner extends KeyValueScanner {

  private final ResultScanner scanner;
  private final byte[] columnFamilyName;
  private final byte[] columnQualifier;

  /**
   * Construct a new InternalKeyValueScanner based on a RegionScanner. Note that
   * this scanner does do keyValue filtering. That being said, it is best if the
   * provided result scanner is already constrained to the particular KeyValues
   * of interest.
   * 
   * @param scanner The ResultScanner that will be proxied.
   * @param columnFamilyName The particular column family name that holds the values of interest.
   * @param columnQualifier The particular column qualifier that holds the values of interest.
   */
  public ExternalKeyValueScanner(ResultScanner scanner, byte[] columnFamilyName,
      byte[] columnQualifier) {
    super();
    this.scanner = scanner;
    this.columnFamilyName = columnFamilyName;
    this.columnQualifier = columnQualifier;
  }

  @Override
  public boolean next(List<KeyValue> keyValues, int limit) throws IOException {
    Result[] results = scanner.next(limit);

    for (Result r : results) {
      keyValues.addAll(r.getColumn(columnFamilyName, columnQualifier));
    }

    return results.length < limit;
  }
}

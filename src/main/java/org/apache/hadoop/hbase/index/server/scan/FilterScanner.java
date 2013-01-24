package org.apache.hadoop.hbase.index.server.scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * This filter is responsible for limiting generating a filter facade based on
 * the values in another scan. It uses a {@link KeyValueScanner} abstraction to
 * support both local and external scanners as input.
 * 
 */
public class FilterScanner extends FilterBase {

  private static enum Advance {
    NONE_LEFT, CURRENT_MATCHES, CURRENT_IN_FUTURE;
  }

  private final KeyMatcher keyMatcher;
  private final KeyValueScanner scanner;
  private final int maxBufferSize;
  private final List<KeyValue> keyValueBuffer;

  private KeyValue currentIndexKeyValue;
  private boolean rowKeyFinished = false;
  private int bufferIndex = 0;
  private boolean scannerHasMore = true;
  private boolean rowMatches = false;

  /**
   * 
   * @param keyMatcher
   *          Determines whether or not particular rows should be included in
   *          the scan.
   * @param scanner
   *          The scanner which holds the indexed values.
   * @param bufferSize
   *          The maximum batch size of indexed values to pull from the
   *          KeyValueScanner.
   */
  public FilterScanner(KeyMatcher keyMatcher, KeyValueScanner scanner, int bufferSize) {
    super();
    this.keyMatcher = keyMatcher;
    this.scanner = scanner;
    this.maxBufferSize = bufferSize;
    this.keyValueBuffer = new ArrayList<KeyValue>(bufferSize);
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    final Advance advance = advanceToRowKey(buffer, offset, length);

    switch (advance) {
    case NONE_LEFT:
      rowKeyFinished = true;
      return true;

    case CURRENT_MATCHES:
      rowMatches = true;
      return false;

    case CURRENT_IN_FUTURE:
      rowMatches = false;
      return false;

    default:
      throw new IllegalArgumentException("Unsupported Advance type " + advance.name());
    }

  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    // rely on the fact that filterRowKey is always called before this.
    if (rowMatches) {
      return ReturnCode.INCLUDE;
    } else {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    // should only be called on unmatching rows 
    return currentIndexKeyValue;
  }

  private Advance advanceToRowKey(byte[] buffer, int offset, int length) {
    if (currentIndexKeyValue == null) {
      currentIndexKeyValue = getNextIndexKeyValue();
      if (currentIndexKeyValue == null)
        return Advance.NONE_LEFT;
    }

    int compare;
    while ((compare = keyMatcher.compare(buffer, offset, length, currentIndexKeyValue)) < 0) {
      currentIndexKeyValue = getNextIndexKeyValue();
      if (currentIndexKeyValue == null)
        return Advance.NONE_LEFT;
    }

    if (compare == 0) {
      return Advance.CURRENT_MATCHES;
    } else {
      return Advance.CURRENT_IN_FUTURE;
    }
  }

  private KeyValue getNextIndexKeyValue() {
    try {
      // fill buffer if scanner has more.
      if (bufferIndex >= keyValueBuffer.size() && scannerHasMore) {

        scannerHasMore = scanner.next(this.keyValueBuffer, this.maxBufferSize);
        bufferIndex = 0;
      }

      if (bufferIndex >= keyValueBuffer.size())
        return null;

      KeyValue kv = keyValueBuffer.get(bufferIndex);
      bufferIndex++;
      return kv;
    } catch (IOException ex) {
      throw new UnderlyingIndexUseException("Failure while index scanner is attempting to retrieve next index value.", ex);
    }
  }

  @Override
  public boolean filterAllRemaining() {
    return rowKeyFinished;
  }

}

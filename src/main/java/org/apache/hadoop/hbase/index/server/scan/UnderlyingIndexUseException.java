package org.apache.hadoop.hbase.index.server.scan;

/**
 * A RuntimeException that is captured down the stack in situations where
 * existing interfaces don't support exceptions. For example, Filters don't
 * traditionally support exceptions when attempting to check individual rows.
 */
public class UnderlyingIndexUseException extends RuntimeException {

  private static final long serialVersionUID = 2349133773723548784L;

  public UnderlyingIndexUseException(String str, Throwable t) {
    super(str, t);
  }

  public UnderlyingIndexUseException(String str) {
    super(str);
  }

  public UnderlyingIndexUseException(Throwable t) {
    super(t);
  }

}

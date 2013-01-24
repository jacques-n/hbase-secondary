package org.apache.hadoop.hbase.index.server.scan;

import java.io.IOException;

import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A ResultScanner that wraps other result scanners and restarts if it receives
 * a {@link UnknownScannerException} from the underlying ResultScanner. Should
 * only restart the scanner in cases where progress is being made.
 */
public class ResilientResultScanner extends AbstractClientScanner implements ResultScanner {

  private final HTableInterface htable;
  private final Scan scan;
  private ResultScanner resultScanner;
  private byte[] startRowKey;
  private int countSinceException;

  public ResilientResultScanner(HTableInterface htable, Scan scan) throws IOException {
    super();
    this.htable = htable;
    
    this.scan = scan;
    getNewScanner();
  }

  private void resetScanner(UnknownScannerException e) throws IOException{
    // make sure we don't keep doing this.
    if(countSinceException == 0){
      throw e;
    }
    countSinceException = 0;
    
    if(startRowKey != null){
      byte[] newStartRow = new byte[startRowKey.length + 1]; // add zero byte to get next row after currently retrieved row.
      System.arraycopy(startRowKey, 0, newStartRow, 0, startRowKey.length);
      scan.setStartRow(startRowKey);
      resultScanner = htable.getScanner(scan);
    }
    
  }
  
  private void getNewScanner() throws IOException{
    resultScanner = htable.getScanner(scan);
  }
  

  public Result next() throws IOException {
    while(true){
      try{
        Result r = resultScanner.next();
        startRowKey = r.getRow();
        countSinceException++;
        return r;
      }catch(UnknownScannerException e){
        // will throw on second call.
        resetScanner(e);
      }
    }
  }

  public Result[] next(int nbRows) throws IOException {
    while(true){
      try{
        Result[] r = resultScanner.next(nbRows);
        startRowKey = r[r.length-1].getRow();
        countSinceException += r.length;
        return r;
      }catch(UnknownScannerException e){
        // will throw on second call.
        resetScanner(e);
      }
    }
  }

  public void close() {
    resultScanner.close();
  }

}

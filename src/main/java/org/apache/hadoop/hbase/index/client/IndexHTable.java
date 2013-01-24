package org.apache.hadoop.hbase.index.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.index.common.OptionsHelper;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

class IndexHTable implements HTableInterface{

  
  private HTable htable;
  private TableIndexOptions indexOptions;
  private HTablePool pool;
  
  public IndexHTable(Configuration config, byte[] tableName, IndexHTablePool pool) throws IOException{
    this.htable = new HTable(config, tableName);
    refreshDescriptor();
    this.pool = pool;
  }
  

  public void refreshDescriptor() throws IOException{
    HTableDescriptor desc = htable.getTableDescriptor();
    TableIndexOptions newOptions = OptionsHelper.getOptionSet(desc);
    if(indexOptions.equals(newOptions)) return;
    
    
  }
  
  public byte[] getTableName() {
    return htable.getTableName();
  }

  public Configuration getConfiguration() {
    return htable.getConfiguration();
  }

  public HTableDescriptor getTableDescriptor() throws IOException {
    return htable.getTableDescriptor();
  }

  public boolean exists(Get get) throws IOException {
    if(get.getRow().length > 0){
      return htable.exists(get);
    }
    
    Filter f = get.getFilter();
    
    
  }
  

  public void batch(List<? extends Row> actions, Object[] results) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    
  }

  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }

  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  public Result get(Get get) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public Result[] get(List<Get> gets) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public ResultScanner getScanner(byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public void put(Put put) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public void put(List<Put> puts) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  public void delete(Delete delete) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public void delete(List<Delete> deletes) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  public void mutateRow(RowMutations rm) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public Result append(Append append) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public Result increment(Increment increment) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      boolean writeToWAL) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  public boolean isAutoFlush() {
    // TODO Auto-generated method stub
    return false;
  }

  public void flushCommits() throws IOException {
    // TODO Auto-generated method stub
    
  }

  public void close() throws IOException {
    htable.close();
  }

  public RowLock lockRow(byte[] row) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public void unlockRow(RowLock rl) throws IOException {
    // TODO Auto-generated method stub
    
  }

  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, Call<T, R> callable) throws IOException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, Call<T, R> callable, Callback<R> callback)
      throws IOException, Throwable {
    // TODO Auto-generated method stub
    
  }

  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    
  }

  public void setAutoFlush(boolean autoFlush) {
    // TODO Auto-generated method stub
    
  }

  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    // TODO Auto-generated method stub
    
  }

  public long getWriteBufferSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    // TODO Auto-generated method stub
    
  }

  
  
}

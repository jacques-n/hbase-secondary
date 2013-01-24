package org.apache.hadoop.hbase.index.common;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.index.common.protobuf.IndexOptionProtos.TableIndexOptions;

import com.google.protobuf.InvalidProtocolBufferException;

public class OptionsHelper {

  public static TableIndexOptions getOptionSet(HTableDescriptor descriptor) throws InvalidProtocolBufferException{
    byte[] indexOptionsData = descriptor.getValue(IndexingConstants.TABLE_DESCRIPTOR_KEY);
    if (indexOptionsData == null) {   
      return TableIndexOptions.getDefaultInstance();
    } else {
      TableIndexOptions indexOptions = TableIndexOptions.parseFrom(indexOptionsData);
      return indexOptions;
    }
  }
}

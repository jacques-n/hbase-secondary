package org.apache.hadoop.hbase.index.common;

import java.nio.charset.Charset;

import org.apache.hadoop.hbase.HConstants;

public class IndexingConstants {
  private IndexingConstants(){}

  private static enum IndexFamilyType{
    INDEX_DATA("idxd"),
    INDEX_JOURNAL("idxj"),
    ;
    
    public final byte[] family;
    public final String familyName;
    
    private IndexFamilyType(String name){
      familyName = name;
      family = name.getBytes(UTF8);
    }
    
  }
  
  private static final Charset UTF8 = Charset.forName(HConstants.UTF8_ENCODING);
  public static final byte[] INDEX_LOG_UPDATE_TABLE = ".SECOND_INDEX_LOG.".getBytes(UTF8);
  public static final long TABLE_DESCRIPTOR_UPDATE_FREQUENCY_MS = 1000*60*3; // 3min default. 
  public static final byte[] TABLE_DESCRIPTOR_KEY = "INDEX_METADATA".getBytes(UTF8);
  
//
//  public static byte[] getColumnFamily(byte[] baseColumnFamily){
//    return null;
//  }
 
}

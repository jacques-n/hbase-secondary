package org.apache.hadoop.hbase.index;

import java.util.Iterator;
import java.util.ListIterator;

import org.apache.hadoop.hbase.client.Mutation;

public interface MutationAcceptor {
  public void addMutation(Mutation m);

  public static class IteratorAcceptor implements MutationAcceptor{
    ListIterator<Mutation> iter;
    
    public IteratorAcceptor(Iterator<Mutation> iter){
      
    }
    
    public void addMutation(Mutation m) {
      iter.add(m);
    }
    
  }
}

package org.apache.hadoop.hbase.index;

import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;

public interface MutationFilter {

  public void addMutation(Mutation m, List<Mutation> outgoingMutations, List<byte[]> rowKeysIncluded);
}

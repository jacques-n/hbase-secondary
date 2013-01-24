package org.apache.hadoop.hbase.index;

public interface MutationTransformer {

   public MutationSet convertMutationSet(MutationSet mutationSet);
}

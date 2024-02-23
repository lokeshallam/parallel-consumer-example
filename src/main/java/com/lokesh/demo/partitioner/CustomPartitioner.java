package com.lokesh.demo.partitioner;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        int numPartitions = partitions.size();

        if (keyBytes == null || (!(key instanceof String))) {
            throw new InvalidRecordException("Record key is not a valid String");
        }

        //Your custom logic here
        if (((String) key).startsWith("Tenant_1")) {
            return Math.abs(numPartitions - 1);
        } else if (((String) key).startsWith("Tenant_2")) {

            return Math.abs(numPartitions - 2);
        }

        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

}

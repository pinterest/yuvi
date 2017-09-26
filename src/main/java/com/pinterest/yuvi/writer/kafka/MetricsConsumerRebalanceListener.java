package com.pinterest.yuvi.writer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MetricsConsumerRebalanceListener implements ConsumerRebalanceListener {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConsumerRebalanceListener.class);

  MetricsConsumerRebalanceListener() {
    LOG.info("Created a metrics rebalance listener.");
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    LOG.info("Revoked partitions {}", partitions);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOG.info("Assigned partitions {}", partitions);
  }
}

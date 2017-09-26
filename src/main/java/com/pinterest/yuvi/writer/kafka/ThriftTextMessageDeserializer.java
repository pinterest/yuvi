package com.pinterest.yuvi.writer.kafka;


import com.pinterest.yuvi.thrift.TextMessage;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ThriftTextMessageDeserializer implements Deserializer<TextMessage> {

  private static Logger LOG = LoggerFactory.getLogger(ThriftTextMessageDeserializer.class);
  private final TDeserializer deserializer = new TDeserializer();

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
  }

  @Override
  public TextMessage deserialize(String topic, byte[] data) {
    try {
      TextMessage textMessage = TextMessage.class.newInstance();
      deserializer.deserialize(textMessage, data);
      return textMessage;
    } catch (Exception e) {
      LOG.error("Failed to parse metrics " + data.toString(), e);
      return null;
    }
  }

  @Override
  public void close() {
  }
}

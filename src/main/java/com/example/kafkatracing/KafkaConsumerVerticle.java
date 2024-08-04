package com.example.kafkatracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaConsumerVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "latest");
    config.put("enable.auto.commit", "true");

    // use consumer for interacting with Apache Kafka
    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

    consumer.handler(record -> {
      log.info("Before entering future call");
      log.info("traceId={}", Span.current().getSpanContext().getTraceId());
      log.info("spanId={}", Span.current().getSpanContext().getSpanId());

      vertx.executeBlocking(() -> {
        try {
          //some db/network call
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
        return null;
      }).onComplete(result -> {
        log.info("After future call");
        log.info("headers={}", record.headers());
        log.info("key={}", record.key());
        log.info("value={}", record.value());
        log.info("traceId={}", Span.current().getSpanContext().getTraceId());
        log.info("spanId={}", Span.current().getSpanContext().getSpanId());
      });
    });

    consumer.subscribe("hello-world.topic");
  }
}

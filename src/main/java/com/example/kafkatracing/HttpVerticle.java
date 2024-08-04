package com.example.kafkatracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HttpVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    HttpServerOptions options = new HttpServerOptions().setTracingPolicy(TracingPolicy.ALWAYS);

    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "1");
    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);

    vertx.createHttpServer(options).requestHandler(req -> {
      SpanContext spanContext = Span.current().getSpanContext();
      log.info("TraceId: {} , SpanId: {} ,Request received", spanContext.getTraceId(), spanContext.getSpanId());


      KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("hello-world.topic", "key", "Hello World");

      producer.send(record).onSuccess(response -> {
          req.response()
            .putHeader("content-type", "text/plain")
            .setStatusCode(200)
            .end(record.headers().toString());
        })
        .onFailure(error -> req.response().setStatusCode(500).end(error.getMessage()));


    }).listen(8888).onComplete(http -> {
      if (http.succeeded()) {
        startPromise.complete();
        log.info("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

}

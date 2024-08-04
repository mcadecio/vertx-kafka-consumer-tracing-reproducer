package com.example.kafkatracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.vertx.core.*;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.tracing.opentelemetry.OpenTelemetryTracingFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainVerticle extends AbstractVerticle {


  public static void main(String[] args) {
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder().build();
    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
      .setTracerProvider(sdkTracerProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .buildAndRegisterGlobal();
    Vertx vertx = Vertx.builder()
      .withTracer(new OpenTelemetryTracingFactory(openTelemetry))
      .build();

    Runtime.getRuntime().addShutdownHook(new Thread(vertx::close));

    vertx.deployVerticle(new HttpVerticle()).onFailure(error -> log.error("Failed to deploy http verticle", error));
    vertx.deployVerticle(new KafkaConsumerVerticle()).onFailure(error -> log.error("Failed to deploy kafka verticle", error));
  }
}

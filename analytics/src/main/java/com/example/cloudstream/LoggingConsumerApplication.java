package com.example.cloudstream;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

@SpringBootApplication
@EnableBinding(Sink.class)
public class LoggingConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(LoggingConsumerApplication.class, args);
  }

  // kafka-topics --list --zookeeper localhost:2181 --> topic "input"
  @StreamListener(Sink.INPUT)
  public void handle(Person person) {
    System.out.println("Received: " + person);
  }

  @Data
  @NoArgsConstructor
  @ToString
  public static class Person {
    private String name;
  }
}

package com.microservices.twitter.to.kafka.service;

import com.microservices.config.TwitterToKafkaConfigData;

import com.microservices.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;


@SpringBootApplication
@ComponentScan(basePackages = "com.microservices")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterToKafkaConfigData configData;
    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKafkaConfigData configData, StreamRunner streamRunner) {
        this.configData = configData;
        this.streamRunner = streamRunner;
    }

    // CommandLineRunner overrides the run method.
    // Here it is being used as a simple bean initialization approach
    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info(configData.getWelcomeMessage());
        LOG.info(Arrays.toString(configData.getTwitterKeywords().toArray()));
        streamRunner.start();
    }
}

package com.microservices.twitter.to.kafka.service.runner.impl;

import com.microservices.config.TwitterToKafkaConfigData;
import com.microservices.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.twitter.to.kafka.service.runner.StreamRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnExpression("not ${twitter-to-kafka-service.enable-v2-tweets} && ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaConfigData configData;
    private final TwitterKafkaStatusListener statusListener;
    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaConfigData configData, TwitterKafkaStatusListener statusListener) {
        this.configData = configData;
        this.statusListener = statusListener;
    }

    @Override
    public void start() {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(statusListener);
        addFilter();
    }

    // This method will not be called if the bean is in @Prototype scope, only @Singleton
    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            LOG.info("---> Closing Twitter Stream!");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}

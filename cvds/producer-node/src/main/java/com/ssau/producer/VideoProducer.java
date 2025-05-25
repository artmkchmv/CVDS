package com.ssau.producer.service;

import com.ssau.producer.utils.ConfigLoader;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class VideoProducer {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    public static void main(String[] args) {
        try {
            Properties appProps = ConfigLoader.loadDefault();

            String topic = appProps.getProperty("kafka.topic");
            Properties adminProps = new Properties();
            adminProps.put("bootstrap.servers", appProps.getProperty("kafka.bootstrap.servers"));

            try (AdminClient adminClient = AdminClient.create(adminProps)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                log.info("Topic '{}' created or already exists.", topic);
            } catch (Exception e) {
                log.error("Failed to create topic '{}': {}", topic, e.getMessage());
            }

            Producer<String, String> producer = createKafkaProducer(appProps);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Kafka producer and thread pool...");
                producer.close();
                executor.shutdown();
            }));

            startCameraThreads(appProps, producer);

        } catch (IOException e) {
            log.error("Failed to load configuration: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Application error: {}", e.getMessage(), e);
        }
    }

    private static Producer<String, String> createKafkaProducer(Properties appProps) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", appProps.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("acks", appProps.getProperty("kafka.acks"));
        kafkaProps.put("retries", appProps.getProperty("kafka.retries"));
        kafkaProps.put("batch.size", appProps.getProperty("kafka.batch.size"));
        kafkaProps.put("linger.ms", appProps.getProperty("kafka.linger.ms"));
        kafkaProps.put("max.request.size", appProps.getProperty("kafka.max.request.size"));
        kafkaProps.put("compression.type", appProps.getProperty("kafka.compression.type"));
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(kafkaProps);
    }

    private static void startCameraThreads(Properties appProps, Producer<String, String> producer) {
        String[] cameraIds = appProps.getProperty("camera.id").split(",");
        String[] cameraUrls = appProps.getProperty("camera.url").split(",");
        String topic = appProps.getProperty("kafka.topic");

        if (cameraIds == null || cameraUrls == null || cameraIds.length != cameraUrls.length) {
            throw new IllegalArgumentException("Invalid camera configuration: number of IDs and URLs must match and be non-null");
        }

        log.info("Starting {} camera stream threads...", cameraIds.length);

        for (int i = 0; i < cameraIds.length; i++) {
            String camId = cameraIds[i].trim();
            String url = cameraUrls[i].trim();
            executor.submit(new VideoEventCreator(camId, url, producer, topic));
        }
    }
}

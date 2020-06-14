package com.rurocker.demo.kafkasecurity.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.rurocker.demo.kafkasecurity.dto.TestDto;

@Component
public class TestConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = "avro-topic")
    public void listen(@Payload final TestDto dto, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) final int partition,
            @Header(KafkaHeaders.OFFSET) final long offset) {
        logger.info("receive message with partition {} and offset {}", partition, offset);
        logger.info("testId: {} and testName: {}", dto.getTestId(), dto.getTestName());
    }
}

package com.rurocker.demo.kafkasecurity.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.rurocker.demo.kafkasecurity.config.KafkaConfiguration;
import com.rurocker.demo.kafkasecurity.dto.TestDto;

@Component
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaConfiguration config;

    public void sendMessage(final TestDto dto) {
        kafkaTemplate.send(config.getTopicName(), String.valueOf(dto.getTestId()), dto);
    }

}

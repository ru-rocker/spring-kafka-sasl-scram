package com.rurocker.demo.kafkasecurity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.rurocker.demo.kafkasecurity.dto.TestDto;
import com.rurocker.demo.kafkasecurity.producer.KafkaProducer;

@SpringBootApplication
public class KafkaSecurityApplication implements CommandLineRunner {

    @Autowired
    private ApplicationContext context;

    public static void main(final String[] args) {
        SpringApplication.run(KafkaSecurityApplication.class, args);
    }

    @Override
    public void run(final String... arg0) throws Exception {
        final KafkaProducer kp = context.getBean(KafkaProducer.class);
        for (int i = 0; i < 2; i++) {
        	TestDto td = TestDto.newBuilder().setTestId(System.currentTimeMillis()).setTestName("Test " + i).build();
        	kp.sendMessage(td);
		}
    }
}

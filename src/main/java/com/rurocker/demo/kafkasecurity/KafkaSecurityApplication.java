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
        System.setProperty("java.security.auth.login.config",
                "/var/private/jaas/jaas-spring-client.conf");
        SpringApplication.run(KafkaSecurityApplication.class, args);
    }

    @Override
    public void run(final String... arg0) throws Exception {
        final KafkaProducer kp = context.getBean(KafkaProducer.class);
        kp.sendMessage(new TestDto(System.currentTimeMillis(), "TEST"));
    }
}

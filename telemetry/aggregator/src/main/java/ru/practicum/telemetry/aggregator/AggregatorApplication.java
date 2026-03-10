package ru.practicum.telemetry.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.practicum.telemetry.aggregator.service.AggregationService;

@SpringBootApplication
public class AggregatorApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AggregatorApplication.class, args);

        AggregationService aggregator = context.getBean(AggregationService.class);
        aggregator.start();
    }
}
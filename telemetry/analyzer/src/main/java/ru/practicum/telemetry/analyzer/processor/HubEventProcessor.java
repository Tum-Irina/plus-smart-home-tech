package ru.practicum.telemetry.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.telemetry.analyzer.config.KafkaProps;
import ru.practicum.telemetry.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventService hubEventService;
    private final KafkaProps kafkaProps;

    @Override
    public void run() {
        try {
            consumer.subscribe(java.util.List.of(kafkaProps.getHubsTopic()));
            log.info("HubEventProcessor подписался на топик {}", kafkaProps.getHubsTopic());

            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(Duration.ofMillis(kafkaProps.getPollTimeoutMs()));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    log.debug("Получено событие хаба: hubId={}, type={}",
                            event.getHubId(), event.getPayload());

                    hubEventService.process(event);
                }

                consumer.commitAsync();
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor получил сигнал на завершение");
        } catch (Exception e) {
            log.error("Ошибка в HubEventProcessor", e);
        } finally {
            consumer.commitSync();
            consumer.close();
            log.info("HubEventProcessor закрыт");
        }
    }
}
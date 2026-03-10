package ru.practicum.telemetry.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.practicum.telemetry.collector.service.handle.grpc.HubGrpcEventHandler;
import ru.practicum.telemetry.collector.service.handle.grpc.SensorGrpcEventHandler;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class GrpcEventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<SensorEventProto.PayloadCase, SensorGrpcEventHandler> sensorEventHandlers;
    private final Map<HubEventProto.PayloadCase, HubGrpcEventHandler> hubEventHandlers;

    public GrpcEventController(
            Set<SensorGrpcEventHandler> sensorEventHandlers,
            Set<HubGrpcEventHandler> hubEventHandlers
    ) {
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(
                        SensorGrpcEventHandler::getMessageType,
                        Function.identity()
                ));

        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(
                        HubGrpcEventHandler::getMessageType,
                        Function.identity()
                ));

        log.info("Инициализировано {} gRPC обработчиков для событий датчиков", sensorEventHandlers.size());
        log.info("Инициализировано {} gRPC обработчиков для событий хабов", hubEventHandlers.size());
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено gRPC событие от датчика: id={}, hubId={}, type={}",
                    request.getId(), request.getHubId(), request.getPayloadCase());

            SensorEventProto.PayloadCase payloadCase = request.getPayloadCase();

            if (payloadCase == SensorEventProto.PayloadCase.PAYLOAD_NOT_SET) {
                throw new IllegalArgumentException("Тип события не указан в payload");
            }

            SensorGrpcEventHandler handler = sensorEventHandlers.get(payloadCase);
            if (handler == null) {
                throw new IllegalArgumentException("Не найден обработчик для события: " + payloadCase);
            }

            handler.handle(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("Событие от датчика успешно обработано");
        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события датчика", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено gRPC событие от хаба: hubId={}, type={}",
                    request.getHubId(), request.getPayloadCase());

            HubEventProto.PayloadCase payloadCase = request.getPayloadCase();

            if (payloadCase == HubEventProto.PayloadCase.PAYLOAD_NOT_SET) {
                throw new IllegalArgumentException("Тип события не указан в payload");
            }

            HubGrpcEventHandler handler = hubEventHandlers.get(payloadCase);
            if (handler == null) {
                throw new IllegalArgumentException("Не найден обработчик для события: " + payloadCase);
            }

            handler.handle(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("Событие от хаба успешно обработано");
        } catch (Exception e) {
            log.error("Ошибка обработки gRPC события хаба", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
            ));
        }
    }
}
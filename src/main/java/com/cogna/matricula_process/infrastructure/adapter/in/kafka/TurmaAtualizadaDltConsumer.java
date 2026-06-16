package com.cogna.matricula_process.infrastructure.adapter.in.kafka;

import com.cogna.matricula_process.infrastructure.config.MdcContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Consumer do Dead Letter Topic (DLT) do fluxo turma-atualizada.
 *
 * Toda mensagem que esgotar as tentativas de retry é encaminhada pelo
 * {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer} para o tópico
 * "turma-atualizada.DLT". Este consumer registra os detalhes da falha para
 * rastreabilidade e futura análise/reprocessamento manual.
 */
@Slf4j
@Component
public class TurmaAtualizadaDltConsumer {

    private static final String HEADER_EXCEPTION_MESSAGE  = "kafka_dlt-exception-message";
    private static final String HEADER_EXCEPTION_FQCN     = "kafka_dlt-exception-fqcn";
    private static final String HEADER_ORIGINAL_TOPIC     = "kafka_dlt-original-topic";
    private static final String HEADER_ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    private static final String HEADER_ORIGINAL_OFFSET    = "kafka_dlt-original-offset";

    @KafkaListener(
            topics = "${kafka.topics.turma-atualizada}.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(ConsumerRecord<String, Object> record) {
        String originalTopic     = extrairHeader(record, HEADER_ORIGINAL_TOPIC);
        String originalPartition = extrairHeader(record, HEADER_ORIGINAL_PARTITION);
        String originalOffset    = extrairHeader(record, HEADER_ORIGINAL_OFFSET);
        String exceptionFqcn     = extrairHeader(record, HEADER_EXCEPTION_FQCN);
        String exceptionMessage  = extrairHeader(record, HEADER_EXCEPTION_MESSAGE);

        try (MdcContext ignored = MdcContext.of(
                MdcContext.CORRELATION_ID, UUID.randomUUID().toString(),
                "dlt.originalTopic",       originalTopic,
                "dlt.originalPartition",   originalPartition,
                "dlt.originalOffset",      originalOffset,
                "dlt.exceptionType",       exceptionFqcn
        )) {
            log.error(
                    "Mensagem enviada ao DLT após esgotar retries | payload={} | exceptionMessage={}",
                    record.value(),
                    exceptionMessage
            );
        }
    }

    private String extrairHeader(ConsumerRecord<?, ?> record, String headerKey) {
        Header header = record.headers().lastHeader(headerKey);
        if (header == null) {
            return "N/A";
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}

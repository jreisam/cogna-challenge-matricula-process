package com.cogna.matricula_process.infrastructure.adapter.in.kafka;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TurmaAtualizadaConsumer {

    private final ProcessarTurmaAtualizadaUseCase processarTurmaAtualizadaUseCase;

    @KafkaListener(
            topics = "${kafka.topics.turma-atualizada}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(@Payload TurmaAtualizadaEvent event) {
        MdcContext.popular(event.getBusinessKey(), event.getCicloId());
        try {
            log.info("Evento recebido do tópico turma-atualizada");
            processarTurmaAtualizadaUseCase.processar(event);
        } finally {
            MdcContext.limpar();
        }
    }
}

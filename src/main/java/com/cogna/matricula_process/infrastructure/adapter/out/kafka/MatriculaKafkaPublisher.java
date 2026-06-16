package com.cogna.matricula_process.infrastructure.adapter.out.kafka;

import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MatriculaKafkaPublisher implements MatriculaEventPublisherPort {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.matricula-atualizada}")
    private String topico;

    @Override
    public void publicar(MatriculaAtualizadaEvent evento) {
        log.info("Publicando evento matricula-atualizada | matriculaId={} | topico={}",
                evento.getMatriculaId(), topico);

        kafkaTemplate.send(topico, evento.getMatriculaId(), evento)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Erro ao publicar evento para matriculaId={}: {}",
                                evento.getMatriculaId(), ex.getMessage(), ex);
                    } else {
                        log.info("Evento publicado com sucesso | matriculaId={} | offset={}",
                                evento.getMatriculaId(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}

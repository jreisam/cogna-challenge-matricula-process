package com.cogna.matricula_process.application.port.out;

import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;

public interface MatriculaEventPublisherPort {

    void publicar(MatriculaAtualizadaEvent evento);
}

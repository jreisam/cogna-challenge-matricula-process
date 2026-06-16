package com.cogna.matricula_process.application.port.in;

import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;

public interface ProcessarTurmaAtualizadaUseCase {

    /**
     * Processa o evento de turma atualizada aplicando todas as regras de negócio.
     *
     * @param event evento recebido do tópico turma-atualizada
     */
    void processar(TurmaAtualizadaEvent event);
}

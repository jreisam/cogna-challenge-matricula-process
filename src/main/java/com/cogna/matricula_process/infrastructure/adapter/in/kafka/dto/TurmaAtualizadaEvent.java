package com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TurmaAtualizadaEvent {

    private String businessKey;
    private TurmaEventDTO turma;
    private Long cicloId;
}

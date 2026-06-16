package com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MatriculaAtualizadaEvent {

    private String matriculaId;
    private String alunoId;
    private String businessKey;
    private Long cicloId;
    private List<String> diasDaSemanaAnterior;
    private List<String> diasDaSemanaNovo;
    private LocalDateTime dataAtualizacao;
}

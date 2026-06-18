package com.cogna.matricula_process.infrastructure.adapter.in.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TurmaAtualizadaRequestDTO {

    private String businessKey;
    private Long cicloId;
    private List<String> diasDaSemana;
}

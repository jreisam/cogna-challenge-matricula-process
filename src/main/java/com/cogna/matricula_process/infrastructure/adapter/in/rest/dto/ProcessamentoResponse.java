package com.cogna.matricula_process.infrastructure.adapter.in.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessamentoResponse {

    private String status;
    private String mensagem;
    private String businessKey;
    private Long cicloId;
}

package com.cogna.matricula_process.infrastructure.adapter.in.rest.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TurmaAtualizadaRequest {

    @NotBlank(message = "businessKey é obrigatório")
    private String businessKey;

    @NotNull(message = "cicloId é obrigatório")
    private Long cicloId;

    @NotEmpty(message = "diasDaSemana deve conter ao menos um dia")
    private List<String> diasDaSemana;

    private String horarioInicio;

    private String horarioFim;
}

package com.cogna.matricula_process.infrastructure.adapter.out.rest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CicloResponse {

    private Long id;
    private boolean ativo;

    @JsonProperty("dataInicioCaptura")
    private LocalDate dataInicioCaptura;

    @JsonProperty("dataFimCaptura")
    private LocalDate dataFimCaptura;
}

package com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TurmaEventDTO {

    private String codigo;
    private List<String> diasDaSemana;
    private String horarioInicio;
    private String horarioFim;
    private Integer vagas;
}

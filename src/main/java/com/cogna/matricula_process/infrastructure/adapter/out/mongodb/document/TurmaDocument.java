package com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TurmaDocument {

    private String codigo;
    private List<String> diasDaSemana;
    private String horarioInicio;
    private String horarioFim;
}

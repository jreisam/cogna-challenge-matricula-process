package com.cogna.matricula_process.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Matricula {

    private String id;
    private String alunoId;
    private String businessKey;
    private StatusMatricula status;
    private Turma turma;
    private Long cicloId;
    private LocalDateTime dataMatricula;
}

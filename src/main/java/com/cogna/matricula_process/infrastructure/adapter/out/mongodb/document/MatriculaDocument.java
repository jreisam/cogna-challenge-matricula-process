package com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "matriculas")
public class MatriculaDocument {

    @Id
    private String id;

    private String alunoId;

    @Indexed
    private String businessKey;

    private String status;

    private TurmaDocument turma;

    private Long cicloId;

    private LocalDateTime dataMatricula;
}

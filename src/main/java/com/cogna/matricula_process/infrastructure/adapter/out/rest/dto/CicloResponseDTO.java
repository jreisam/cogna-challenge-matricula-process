package com.cogna.matricula_process.infrastructure.adapter.out.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CicloResponseDTO {

    private Long id;
    private boolean ativo;
    private LocalDate dataInicioCaptura;
    private LocalDate dataFimCaptura;
}

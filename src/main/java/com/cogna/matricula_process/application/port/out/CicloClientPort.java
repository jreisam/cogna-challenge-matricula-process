package com.cogna.matricula_process.application.port.out;

import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;

import java.util.Optional;

public interface CicloClientPort {

    /**
     * Busca os dados de um ciclo pelo seu ID.
     *
     * @param cicloId ID do ciclo a ser consultado
     * @return Optional com os dados do ciclo, ou Optional.empty() se não encontrado (404)
     */
    Optional<CicloResponseDTO> buscarPorId(Long cicloId);
}
package com.cogna.matricula_process.domain.exception;

public class CicloNaoEncontradoException extends RuntimeException {

    public CicloNaoEncontradoException(Long cicloId) {
        super("Ciclo não encontrado para o id: " + cicloId);
    }
}


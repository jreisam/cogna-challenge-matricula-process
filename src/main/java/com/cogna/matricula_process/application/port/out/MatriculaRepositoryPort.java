package com.cogna.matricula_process.application.port.out;

import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;

import java.util.List;

public interface MatriculaRepositoryPort {

    /**
     * Busca todas as matrículas por businessKey e status.
     *
     * @param businessKey chave de negócio da turma
     * @param status      status das matrículas a filtrar
     * @return lista de matrículas encontradas
     */
    List<Matricula> buscarPorBusinessKeyEStatus(String businessKey, StatusMatricula status);

    /**
     * Salva (cria ou atualiza) uma matrícula.
     *
     * @param matricula entidade a ser persistida
     * @return entidade persistida com id preenchido
     */
    Matricula salvar(Matricula matricula);

    /**
     * Busca todas as matrículas ativas por businessKey.
     *
     * @param businessKey chave de negócio da turma
     * @return lista de matrículas ativas encontradas
     */
    List<Matricula> buscarAtivasPorBusinessKey(String businessKey);
}


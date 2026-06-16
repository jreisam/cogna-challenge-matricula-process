package com.cogna.matricula_process.infrastructure.adapter.out.mongodb.repository;

import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document.MatriculaDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface MatriculaMongoRepository extends MongoRepository<MatriculaDocument, String> {

    /**
     * Busca todas as matrículas por businessKey e status.
     * O Spring Data gera a query automaticamente pela convenção de nome do método.
     *
     * @param businessKey chave de negócio da turma
     * @param status      valor do status como String (ex: "ATIVA", "CANCELADA")
     * @return lista de documentos encontrados
     */
    List<MatriculaDocument> findAllByBusinessKeyAndStatus(String businessKey, String status);
}

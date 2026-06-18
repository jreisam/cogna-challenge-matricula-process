package com.cogna.matricula_process.infrastructure.adapter.out.mongodb;

import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.domain.model.Turma;
import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document.MatriculaDocument;
import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document.TurmaDocument;
import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.repository.MatriculaMongoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MatriculaMongoAdapter implements MatriculaRepositoryPort {

    private final MatriculaMongoRepository repository;

    @Override
    public List<Matricula> buscarPorBusinessKeyEStatus(String businessKey, StatusMatricula status) {
        log.info("Buscando matrículas no MongoDB | businessKey={} | status={}", businessKey, status);

        List<Matricula> matriculas = repository
                .findAllByBusinessKeyAndStatus(businessKey, status.name())
                .stream()
                .map(this::toModel)
                .toList();

        log.info("{} matrícula(s) encontrada(s) no MongoDB para businessKey={}", matriculas.size(), businessKey);
        return matriculas;
    }

    @Override
    public Matricula salvar(Matricula matricula) {
        log.info("Salvando matrícula no MongoDB | id={} | alunoId={}", matricula.getId(), matricula.getAlunoId());
        MatriculaDocument salvo = repository.save(toDocument(matricula));
        log.info("Matrícula salva com sucesso | id={}", salvo.getId());
        return toModel(salvo);
    }

    /**
     * Busca todas as matrículas ativas por businessKey.
     *
     * @param businessKey chave de negócio da turma
     * @return lista de matrículas ativas encontradas
     */
    @Override
    public List<Matricula> buscarAtivasPorBusinessKey(String businessKey) {
        log.info("Buscando matrículas ativas no MongoDB | businessKey={}", businessKey);

        List<Matricula> matriculasAtivas = repository
                .findAllByBusinessKeyAndStatus(businessKey, StatusMatricula.ATIVA.name())
                .stream()
                .map(this::toModel)
                .toList();

        log.info("{} matrícula(s) ativas encontrada(s) no MongoDB para businessKey={}", matriculasAtivas.size(), businessKey);
        return matriculasAtivas;
    }

    // -------------------------------------------------------------------------
    // Mappers
    // -------------------------------------------------------------------------

    private Matricula toModel(MatriculaDocument doc) {
        return Matricula.builder()
                .id(doc.getId())
                .alunoId(doc.getAlunoId())
                .businessKey(doc.getBusinessKey())
                .status(resolverStatus(doc.getId(), doc.getStatus()))
                .turma(resolverTurma(doc.getId(), doc.getTurma()))
                .cicloId(doc.getCicloId())
                .dataMatricula(doc.getDataMatricula())
                .build();
    }

    private MatriculaDocument toDocument(Matricula model) {
        return MatriculaDocument.builder()
                .id(model.getId())
                .alunoId(model.getAlunoId())
                .businessKey(model.getBusinessKey())
                .status(model.getStatus().name())
                .turma(TurmaDocument.builder()
                        .codigo(model.getTurma().getCodigo())
                        .diasDaSemana(model.getTurma().getDiasDaSemana())
                        .horarioInicio(model.getTurma().getHorarioInicio())
                        .horarioFim(model.getTurma().getHorarioFim())
                        .build())
                .cicloId(model.getCicloId())
                .dataMatricula(model.getDataMatricula())
                .build();
    }

    /**
     * Converte o status String do documento para o enum StatusMatricula.
     * Retorna null e loga um aviso caso o valor armazenado seja inválido,
     * evitando que um documento corrompido derrube o processamento inteiro.
     */
    private StatusMatricula resolverStatus(String docId, String status) {
        try {
            return StatusMatricula.valueOf(status);
        } catch (IllegalArgumentException | NullPointerException ex) {
            log.warn("Status inválido ou nulo no documento id={} | valor='{}'. Retornando null.", docId, status);
            return null;
        }
    }

    /**
     * Converte o TurmaDocument aninhado para o model Turma.
     * Retorna um Turma vazio e loga um aviso caso turma seja null no documento,
     * evitando NullPointerException por dados incompletos no banco.
     */
    private Turma resolverTurma(String docId, TurmaDocument turmaDocument) {
        if (turmaDocument == null) {
            log.warn("Campo 'turma' ausente no documento id={}. Retornando Turma vazia.", docId);
            return Turma.builder().build();
        }
        return Turma.builder()
                .codigo(turmaDocument.getCodigo())
                .diasDaSemana(turmaDocument.getDiasDaSemana())
                .horarioInicio(turmaDocument.getHorarioInicio())
                .horarioFim(turmaDocument.getHorarioFim())
                .build();
    }
}

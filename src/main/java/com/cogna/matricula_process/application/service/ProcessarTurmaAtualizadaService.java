package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;
import com.cogna.matricula_process.infrastructure.config.MdcContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessarTurmaAtualizadaService implements ProcessarTurmaAtualizadaUseCase {

    private final CicloClientPort cicloClientPort;
    private final MatriculaRepositoryPort matriculaRepositoryPort;
    private final MatriculaEventPublisherPort matriculaEventPublisherPort;

    /**
     * Processa o evento de turma atualizada aplicando todas as regras de negócio.
     *
     * @param event evento recebido do tópico turma-atualizada
     */
    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada");

        // Regra 1 — Buscar e validar o ciclo vigente
        Optional<CicloResponseDTO> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo não encontrado (404). Evento descartado.");
            return;
        }

        CicloResponseDTO ciclo = cicloOpt.get();

        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo não está vigente (ativo={}, dataInicio={}, dataFim={}). Evento descartado.",
                    ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo vigente. Buscando matrículas ATIVAS.");

        // Regra 3 — Buscar matrículas ATIVAS com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ATIVA(s) encontrada(s).", matriculas.size());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        // Regra 4 — Comparar dias e atualizar apenas as matrículas com dias diferentes
        for (Matricula matricula : matriculas) {
            try (MdcContext ignored = MdcContext.of(
                    MdcContext.MATRICULA_ID, matricula.getId(),
                    MdcContext.ALUNO_ID,     matricula.getAlunoId()
            )) {
                List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

                if (diasIguais(diasAtuais, novosDias)) {
                    log.info("Matrícula já possui os mesmos dias. Nenhuma ação necessária.");
                    continue;
                }

                log.info("Dias diferentes detectados. Atualizando: {} → {}", diasAtuais, novosDias);

                List<String> diasAnteriores = List.copyOf(diasAtuais);

                // Atualiza os dias da turma na matrícula
                matricula.getTurma().setDiasDaSemana(novosDias);

                // Jornada 4 — Persiste a matrícula atualizada
                Matricula matriculaAtualizada = matriculaRepositoryPort.salvar(matricula);

                // Jornada 5 — Publica evento `matricula-atualizada`
                MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                        .matriculaId(matriculaAtualizada.getId())
                        .alunoId(matriculaAtualizada.getAlunoId())
                        .businessKey(matriculaAtualizada.getBusinessKey())
                        .cicloId(event.getCicloId())
                        .diasDaSemanaAnterior(diasAnteriores)
                        .diasDaSemanaNovo(novosDias)
                        .dataAtualizacao(LocalDateTime.now())
                        .build();

                matriculaEventPublisherPort.publicar(eventoSaida);

                log.info("Evento matricula-atualizada publicado.");
            }
        }

        log.info("Processamento concluído.");
    }

    /**
     * Ciclo é vigente se:
     * - ativo == true
     * - dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponseDTO ciclo) {
        if (!ciclo.isAtivo()) {
            return false;
        }
        LocalDate hoje = LocalDate.now();
        return !hoje.isBefore(ciclo.getDataInicioCaptura())
                && hoje.isBefore(ciclo.getDataFimCaptura());
    }

    /**
     * Compara dois conjuntos de dias da semana ignorando ordem.
     */
    private boolean diasIguais(List<String> diasAtuais, List<String> novosDias) {
        return new HashSet<>(diasAtuais).equals(new HashSet<>(novosDias));
    }
}

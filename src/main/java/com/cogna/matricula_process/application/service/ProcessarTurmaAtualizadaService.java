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
        log.info("Iniciando processamento do evento turma-atualizada | businessKey={} | cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Regra 1 — Buscar e validar o ciclo vigente
        Optional<CicloResponseDTO> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado (404). Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponseDTO ciclo = cicloOpt.get();

        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, dataInicio={}, dataFim={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ATIVAS para businessKey={}",
                ciclo.getId(), event.getBusinessKey());

        // Regra 3 — Buscar matrículas ATIVAS com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ATIVA(s) encontrada(s) para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        // Regra 4 — Comparar dias e atualizar apenas as matrículas com dias diferentes
        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            if (diasIguais(diasAtuais, novosDias)) {
                log.info("Matrícula id={} (alunoId={}) já possui os mesmos dias. Nenhuma ação necessária.",
                        matricula.getId(), matricula.getAlunoId());
                continue;
            }

            log.info("Matrícula id={} (alunoId={}) com dias diferentes. Atualizando: {} → {}",
                    matricula.getId(), matricula.getAlunoId(), diasAtuais, novosDias);

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

            log.info("Evento matricula-atualizada publicado para matriculaId={}", matriculaAtualizada.getId());
        }

        log.info("Processamento concluído para businessKey={}", event.getBusinessKey());
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

package com.cogna.matricula_process.application.service;
package com.cogna.matricula_process.application.service;
package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
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

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada | businessKey={} | cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Regra 1 — Validar ciclo vigente
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado (404). Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, dataInicio={}, dataFim={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ativas para businessKey={}",
                ciclo.getId(), event.getBusinessKey());

        // Regra 3 — Buscar matrículas com businessKey e status ATIVA
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ATIVA(s) encontradas para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        // Regra 4 — Comparar dias e atualizar se necessário
        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            if (diasIguais(diasAtuais, novosDias)) {
                log.info("Matrícula id={} já possui os mesmos dias. Nenhuma ação necessária.", matricula.getId());
                continue;
            }

            log.info("Matrícula id={} com dias diferentes. Atualizando: {} → {}",
                    matricula.getId(), diasAtuais, novosDias);

            List<String> diasAnterior = List.copyOf(diasAtuais);

            matricula.getTurma().setDiasDaSemana(novosDias);
            Matricula matriculaSalva = matriculaRepositoryPort.salvar(matricula);

            MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                    .matriculaId(matriculaSalva.getId())
                    .alunoId(matriculaSalva.getAlunoId())
                    .businessKey(matriculaSalva.getBusinessKey())
                    .cicloId(event.getCicloId())
                    .diasDaSemanaAnterior(diasAnterior)
                    .diasDaSemanaNovo(novosDias)
                    .dataAtualizacao(LocalDateTime.now())
                    .build();

            matriculaEventPublisherPort.publicar(eventoSaida);

            log.info("Evento matricula-atualizada publicado para matriculaId={}", matriculaSalva.getId());
        }

        log.info("Processamento do evento turma-atualizada concluído para businessKey={}", event.getBusinessKey());
    }

    /**
     * Verifica se o ciclo é vigente:
     * - ativo == true
     * - dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponse ciclo) {
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
import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
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

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada. businessKey={} cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Jornada 2 — Busca e valida o ciclo
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado. Evento descartado. businessKey={}",
                    event.getCicloId(), event.getBusinessKey());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();

        // Jornada 3 — Regra de negócio: ciclo vigente
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, janela={} a {}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ativas para businessKey={}",
                event.getCicloId(), event.getBusinessKey());

        // Jornada 3 — Busca matrículas ativas com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrículas ATIVAS encontradas para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();
package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
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

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada | businessKey={} cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Jornada 2 — Buscar ciclo e validar vigência
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado. Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, inicio={}, fim={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ativas para businessKey={}",
                event.getCicloId(), event.getBusinessKey());

        // Jornada 3 — Buscar matrículas ativas com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ativa(s) encontrada(s) para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            // Jornada 3 — Comparação de dias da semana
            if (mesmoDias(diasAtuais, novosDias)) {
                log.info("Matrícula id={} | dias iguais, nenhuma ação necessária.", matricula.getId());
                continue;
            }

            log.info("Matrícula id={} | dias diferentes. Atualizando: {} -> {}",
                    matricula.getId(), diasAtuais, novosDias);

            // Jornada 4 — Atualizar matrícula e persistir
            List<String> diasAnteriores = List.copyOf(diasAtuais);
            matricula.getTurma().setDiasDaSemana(novosDias);
            matriculaRepositoryPort.salvar(matricula);

            // Jornada 5 — Publicar evento de saída
            MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                    .matriculaId(matricula.getId())
                    .alunoId(matricula.getAlunoId())
                    .businessKey(matricula.getBusinessKey())
                    .cicloId(event.getCicloId())
                    .diasDaSemanaAnterior(diasAnteriores)
                    .diasDaSemanaNovo(novosDias)
                    .dataAtualizacao(LocalDateTime.now())
                    .build();

            matriculaEventPublisherPort.publicar(eventoSaida);

            log.info("Evento matricula-atualizada publicado para matriculaId={}", matricula.getId());
        }

        log.info("Processamento concluído para businessKey={}", event.getBusinessKey());
    }

    /**
     * Verifica se o ciclo está vigente:
     * ativo == true E dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponse ciclo) {
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
    private boolean mesmoDias(List<String> diasAtuais, List<String> novosDias) {
        return new HashSet<>(diasAtuais).equals(new HashSet<>(novosDias));
    }
}
        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            // Jornada 3 — Comparação de dias (ignora ordem)
            if (mesmosDias(diasAtuais, novosDias)) {
                log.info("Matrícula id={} já possui os mesmos dias da semana. Nenhuma ação necessária.",
                        matricula.getId());
                continue;
            }

            log.info("Matrícula id={} com dias diferentes. Atualizando: {} -> {}",
                    matricula.getId(), diasAtuais, novosDias);

            List<String> diasAnteriores = List.copyOf(diasAtuais);

            // Jornada 4 — Atualiza os dias da turma na matrícula e persiste
            matricula.getTurma().setDiasDaSemana(novosDias);
            Matricula matriculaAtualizada = matriculaRepositoryPort.salvar(matricula);

            // Jornada 5 — Publica o evento matricula-atualizada
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
     * Regra: ciclo vigente se ativo == true E data atual está dentro da janela de captura.
     * Condição: dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponse ciclo) {
        if (!ciclo.isAtivo()) {
            return false;
        }
        LocalDate hoje = LocalDate.now();
        return !hoje.isBefore(ciclo.getDataInicioCaptura())
                && hoje.isBefore(ciclo.getDataFimCaptura());
    }

    /**
     * Compara dois conjuntos de dias ignorando ordem.
     */
    private boolean mesmosDias(List<String> diasAtuais, List<String> novosDias) {
        return new HashSet<>(diasAtuais).equals(new HashSet<>(novosDias));
    }
}
import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
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

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada. businessKey={}, cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Jornada 2 — Valida ciclo vigente
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado (404). Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();

        // Jornada 3 — Regra de negócio: ciclo vigente?
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, inicio={}, fim={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ATIVAS para businessKey={}",
                event.getCicloId(), event.getBusinessKey());

        // Jornada 3 — Busca matrículas ATIVAS com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ATIVA(s) encontrada(s) para businessKey={}", matriculas.size(), event.getBusinessKey());

        // Jornada 3 — Comparação e atualização de dias
        List<String> novosDias = event.getTurma().getDiasDaSemana();

        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            if (mesmoDias(diasAtuais, novosDias)) {
                log.info("Matrícula id={} (alunoId={}) já possui os mesmos dias. Nenhuma ação necessária.",
                        matricula.getId(), matricula.getAlunoId());
                continue;
            }

            log.info("Matrícula id={} (alunoId={}) com dias diferentes. Atualizando: {} -> {}",
                    matricula.getId(), matricula.getAlunoId(), diasAtuais, novosDias);

            List<String> diasAnteriores = List.copyOf(diasAtuais);

            // Jornada 4 — Atualiza e persiste
            matricula.getTurma().setDiasDaSemana(novosDias);
            Matricula atualizada = matriculaRepositoryPort.salvar(matricula);

            // Jornada 5 — Publica evento de saída
            MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                    .matriculaId(atualizada.getId())
                    .alunoId(atualizada.getAlunoId())
                    .businessKey(atualizada.getBusinessKey())
                    .cicloId(event.getCicloId())
                    .diasDaSemanaAnterior(diasAnteriores)
                    .diasDaSemanaNovo(novosDias)
                    .dataAtualizacao(LocalDateTime.now())
                    .build();

            matriculaEventPublisherPort.publicar(eventoSaida);

            log.info("Evento matricula-atualizada publicado para matriculaId={}", atualizada.getId());
        }

        log.info("Processamento concluído para businessKey={}", event.getBusinessKey());
    }

    private boolean isCicloVigente(CicloResponse ciclo) {
        if (!ciclo.isAtivo()) {
            return false;
        }
        LocalDate hoje = LocalDate.now();
        return !hoje.isBefore(ciclo.getDataInicioCaptura())
                && hoje.isBefore(ciclo.getDataFimCaptura());
    }

    private boolean mesmoDias(List<String> diasAtuais, List<String> diasNovos) {
        return new HashSet<>(diasAtuais).equals(new HashSet<>(diasNovos));
    }
}
@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessarTurmaAtualizadaService implements ProcessarTurmaAtualizadaUseCase {

    private final CicloClientPort cicloClientPort;
    private final MatriculaRepositoryPort matriculaRepositoryPort;
    private final MatriculaEventPublisherPort matriculaEventPublisherPort;

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada: businessKey={}, cicloId={}",
                event.getBusinessKey(), event.getCicloId());
package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
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

    @Override
    public void processar(TurmaAtualizadaEvent event) {
        log.info("Iniciando processamento do evento turma-atualizada: businessKey={}, cicloId={}",
                event.getBusinessKey(), event.getCicloId());

        // Jornada 2: Busca e valida o ciclo
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado (404). Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();

        // Jornada 3: Regra de negócio — validação de ciclo vigente
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, dataInicio={}, dataFim={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ATIVAS para businessKey={}",
                event.getCicloId(), event.getBusinessKey());

        // Busca matrículas ATIVAS com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("Encontradas {} matrículas ATIVAS para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            // Jornada 3: Comparação de dias (usando Set para ignorar ordem)
            if (new HashSet<>(diasAtuais).equals(new HashSet<>(novosDias))) {
                log.info("Matrícula id={} (alunoId={}) já possui os mesmos dias. Nenhuma ação necessária.",
                        matricula.getId(), matricula.getAlunoId());
                continue;
            }

            log.info("Matrícula id={} (alunoId={}) com dias diferentes. Atualizando: {} → {}",
                    matricula.getId(), matricula.getAlunoId(), diasAtuais, novosDias);

            List<String> diasAnteriores = List.copyOf(diasAtuais);

            // Jornada 4: Atualiza os dias e persiste
            matricula.getTurma().setDiasDaSemana(novosDias);
            matriculaRepositoryPort.salvar(matricula);

            // Jornada 5: Publica evento de saída
            MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                    .matriculaId(matricula.getId())
                    .alunoId(matricula.getAlunoId())
                    .businessKey(matricula.getBusinessKey())
                    .cicloId(event.getCicloId())
                    .diasDaSemanaAnterior(diasAnteriores)
                    .diasDaSemanaNovo(novosDias)
                    .dataAtualizacao(LocalDateTime.now())
                    .build();

            matriculaEventPublisherPort.publicar(eventoSaida);

            log.info("Evento matricula-atualizada publicado para matriculaId={}", matricula.getId());
        }

        log.info("Processamento concluído para businessKey={}", event.getBusinessKey());
    }

    /**
     * Verifica se o ciclo está vigente:
     * - ativo == true
     * - dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponse ciclo) {
        if (!ciclo.isAtivo()) {
            return false;
        }
        LocalDate hoje = LocalDate.now();
        return !hoje.isBefore(ciclo.getDataInicioCaptura())
                && hoje.isBefore(ciclo.getDataFimCaptura());
    }
}
        // Jornada 2: Busca e valida o ciclo
        Optional<CicloResponse> cicloOpt = cicloClientPort.buscarPorId(event.getCicloId());

        if (cicloOpt.isEmpty()) {
            log.warn("Ciclo id={} não encontrado (404). Evento descartado.", event.getCicloId());
            return;
        }

        CicloResponse ciclo = cicloOpt.get();

        // Jornada 3: Regra de negócio — valida se o ciclo é vigente
        if (!isCicloVigente(ciclo)) {
            log.warn("Ciclo id={} não está vigente (ativo={}, dataInicioCaptura={}, dataFimCaptura={}). Evento descartado.",
                    ciclo.getId(), ciclo.isAtivo(), ciclo.getDataInicioCaptura(), ciclo.getDataFimCaptura());
            return;
        }

        log.info("Ciclo id={} vigente. Buscando matrículas ATIVAS para businessKey={}",
                event.getCicloId(), event.getBusinessKey());

        // Busca matrículas ATIVAS com o mesmo businessKey
        List<Matricula> matriculas = matriculaRepositoryPort
                .buscarPorBusinessKeyEStatus(event.getBusinessKey(), StatusMatricula.ATIVA);

        log.info("{} matrícula(s) ATIVA(s) encontrada(s) para businessKey={}", matriculas.size(), event.getBusinessKey());

        List<String> novosDias = event.getTurma().getDiasDaSemana();

        for (Matricula matricula : matriculas) {
            List<String> diasAtuais = matricula.getTurma().getDiasDaSemana();

            // Jornada 3: Compara os dias da semana (ordem não importa — usa Set para comparar)
            if (new HashSet<>(diasAtuais).equals(new HashSet<>(novosDias))) {
                log.info("Matrícula id={} (alunoId={}) já possui os mesmos dias. Nenhuma ação necessária.",
                        matricula.getId(), matricula.getAlunoId());
                continue;
            }

            log.info("Matrícula id={} (alunoId={}) com dias diferentes. Atualizando: {} -> {}",
                    matricula.getId(), matricula.getAlunoId(), diasAtuais, novosDias);

            List<String> diasAnteriores = List.copyOf(diasAtuais);

            // Jornada 4: Atualiza os dias da turma e persiste
            matricula.getTurma().setDiasDaSemana(novosDias);
            matriculaRepositoryPort.salvar(matricula);

            // Jornada 5: Publica o evento matricula-atualizada
            MatriculaAtualizadaEvent eventoSaida = MatriculaAtualizadaEvent.builder()
                    .matriculaId(matricula.getId())
                    .alunoId(matricula.getAlunoId())
                    .businessKey(matricula.getBusinessKey())
                    .cicloId(event.getCicloId())
                    .diasDaSemanaAnterior(diasAnteriores)
                    .diasDaSemanaNovo(novosDias)
                    .dataAtualizacao(LocalDateTime.now())
                    .build();

            matriculaEventPublisherPort.publicar(eventoSaida);

            log.info("Evento matricula-atualizada publicado para matriculaId={}", matricula.getId());
        }

        log.info("Processamento concluído para businessKey={}", event.getBusinessKey());
    }

    /**
     * Valida se o ciclo é vigente:
     * - ativo == true
     * - dataInicioCaptura <= hoje < dataFimCaptura
     */
    private boolean isCicloVigente(CicloResponse ciclo) {
        if (!ciclo.isAtivo()) {
            return false;
        }
        LocalDate hoje = LocalDate.now();
        return !hoje.isBefore(ciclo.getDataInicioCaptura())
                && hoje.isBefore(ciclo.getDataFimCaptura());
    }
}

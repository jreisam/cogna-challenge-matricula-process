package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.domain.model.Turma;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaEventDTO;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * C3 — Ciclo vigente, dias da semana iguais: nenhuma ação deve ser realizada.
 *
 * Quando os diasDaSemana da matrícula já são idênticos (ignorando ordem)
 * aos diasDaSemana do evento, a matrícula não deve ser atualizada
 * e nenhum evento deve ser publicado.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("C3 — Ciclo vigente, dias iguais: matrícula não deve ser alterada")
class ProcessarTurmaAtualizadaServiceCicloVigenteDiasIguaisTest {

    @Mock
    private CicloClientPort cicloClientPort;

    @Mock
    private MatriculaRepositoryPort matriculaRepositoryPort;

    @Mock
    private MatriculaEventPublisherPort matriculaEventPublisherPort;

    @InjectMocks
    private ProcessarTurmaAtualizadaService service;

    private CicloResponseDTO cicloVigente;

    @BeforeEach
    void setUp() {
        cicloVigente = new CicloResponseDTO(
                200L,
                true,
                LocalDate.now().minusDays(5),
                LocalDate.now().plusDays(25)
        );
    }

    @Test
    @DisplayName("Não deve atualizar nem publicar quando os dias são exatamente iguais e na mesma ordem")
    void naoDeveAtualizarQuandoDiasIguaisNaMesmaOrdem() {
        // Arrange
        List<String> dias = List.of("SEGUNDA", "QUARTA", "SEXTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-001", dias, "19:00", "22:00", 30);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-001", turmaEvento, 200L);

        Matricula matricula = matriculaComDias("MAT-001", "ALU-001", "BK-001", dias);

        when(cicloClientPort.buscarPorId(200L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula));

        // Act
        service.processar(evento);

        // Assert — repositório foi consultado mas nada foi salvo
        verify(matriculaRepositoryPort, times(1)).buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA);
        verify(matriculaRepositoryPort, never()).salvar(any());

        // Assert — nenhum evento publicado
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Não deve atualizar nem publicar quando os dias são iguais mas em ordem diferente")
    void naoDeveAtualizarQuandoDiasIguaisEmOrdemDiferente() {
        // Arrange — matrícula tem ["QUARTA", "SEGUNDA"], evento traz ["SEGUNDA", "QUARTA"]
        List<String> diasNaMatricula = List.of("QUARTA", "SEGUNDA");
        List<String> diasNoEvento = List.of("SEGUNDA", "QUARTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-001", diasNoEvento, "19:00", "22:00", 30);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-001", turmaEvento, 200L);

        Matricula matricula = matriculaComDias("MAT-002", "ALU-002", "BK-001", diasNaMatricula);

        when(cicloClientPort.buscarPorId(200L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula));

        // Act
        service.processar(evento);

        // Assert
        verify(matriculaRepositoryPort, never()).salvar(any());
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Não deve atualizar nenhuma matrícula quando todas já possuem os mesmos dias")
    void naoDeveAtualizarQuandoTodasMatriculasComDiasIguais() {
        // Arrange — duas matrículas, ambas com os mesmos dias do evento
        List<String> dias = List.of("TERCA", "QUINTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-002", dias, "08:00", "10:00", 25);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-002", turmaEvento, 200L);

        Matricula matricula1 = matriculaComDias("MAT-010", "ALU-010", "BK-002", dias);
        Matricula matricula2 = matriculaComDias("MAT-011", "ALU-011", "BK-002", dias);

        when(cicloClientPort.buscarPorId(200L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-002", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula1, matricula2));

        // Act
        service.processar(evento);

        // Assert — repositório consultado, mas salvar nunca chamado
        verify(matriculaRepositoryPort, times(1)).buscarPorBusinessKeyEStatus("BK-002", StatusMatricula.ATIVA);
        verify(matriculaRepositoryPort, never()).salvar(any());
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Não deve executar nenhuma ação quando não há matrículas ATIVAS para o businessKey")
    void naoDeveExecutarNadaQuandoSemMatriculasAtivas() {
        // Arrange
        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-003", List.of("SEGUNDA"), "07:00", "09:00", 10);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-SEM-MATRICULAS", turmaEvento, 200L);

        when(cicloClientPort.buscarPorId(200L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-SEM-MATRICULAS", StatusMatricula.ATIVA))
                .thenReturn(List.of());

        // Act
        service.processar(evento);

        // Assert
        verify(matriculaRepositoryPort, times(1)).buscarPorBusinessKeyEStatus("BK-SEM-MATRICULAS", StatusMatricula.ATIVA);
        verify(matriculaRepositoryPort, never()).salvar(any());
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    private Matricula matriculaComDias(String id, String alunoId, String businessKey, List<String> dias) {
        return Matricula.builder()
                .id(id)
                .alunoId(alunoId)
                .businessKey(businessKey)
                .status(StatusMatricula.ATIVA)
                .cicloId(200L)
                .dataMatricula(LocalDateTime.now().minusDays(10))
                .turma(Turma.builder()
                        .codigo("TURMA-001")
                        .diasDaSemana(dias)
                        .horarioInicio("19:00")
                        .horarioFim("22:00")
                        .build())
                .build();
    }
}

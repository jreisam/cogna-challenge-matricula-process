package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import com.cogna.matricula_process.domain.model.StatusMatricula;
import com.cogna.matricula_process.domain.model.Turma;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaEventDTO;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * C4 - Ciclo vigente, dias da semana diferentes: deve atualizar e publicar evento.
 *
 * Quando os diasDaSemana da matrícula são diferentes dos diasDaSemana do evento:
 *   - A matrícula deve ser persistida com os novos dias
 *   - Um evento matricula-atualizada deve ser publicado com os dados antes e depois
 */
@ExtendWith(MockitoExtension.class)
@DisplayName(" C4 - Ciclo vigente, dias diferentes: deve atualizar matrícula e publicar evento")
class ProcessarTurmaAtualizadaServiceCicloVigenteDiasDiferentesTest {

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
                300L,
                true,
                LocalDate.now().minusDays(5),
                LocalDate.now().plusDays(25)
        );
    }

    @Test
    @DisplayName("Deve atualizar a matrícula e publicar evento quando os dias são diferentes")
    void deveAtualizarEPublicarQuandoDiasDiferentes() {
        // Arrange
        List<String> diasAntigos = List.of("SEGUNDA", "QUARTA");
        List<String> diasNovos   = List.of("SEGUNDA", "QUARTA", "SEXTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-001", diasNovos, "19:00", "22:00", 30);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-001", turmaEvento, 300L);

        Matricula matricula = matriculaComDias("MAT-001", "ALU-001", "BK-001", diasAntigos);
        Matricula matriculaAtualizada = matriculaComDias("MAT-001", "ALU-001", "BK-001", diasNovos);

        when(cicloClientPort.buscarPorId(300L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula));
        when(matriculaRepositoryPort.salvar(any())).thenReturn(matriculaAtualizada);

        // Act
        service.processar(evento);

        // Assert — matrícula foi salva uma vez
        verify(matriculaRepositoryPort, times(1)).salvar(any());

        // Assert — evento foi publicado uma vez
        ArgumentCaptor<MatriculaAtualizadaEvent> captor = ArgumentCaptor.forClass(MatriculaAtualizadaEvent.class);
        verify(matriculaEventPublisherPort, times(1)).publicar(captor.capture());

        MatriculaAtualizadaEvent eventoPublicado = captor.getValue();
        assertThat(eventoPublicado.getMatriculaId()).isEqualTo("MAT-001");
        assertThat(eventoPublicado.getAlunoId()).isEqualTo("ALU-001");
        assertThat(eventoPublicado.getBusinessKey()).isEqualTo("BK-001");
        assertThat(eventoPublicado.getCicloId()).isEqualTo(300L);
        assertThat(eventoPublicado.getDiasDaSemanaAnterior()).containsExactlyInAnyOrderElementsOf(diasAntigos);
        assertThat(eventoPublicado.getDiasDaSemanaNovo()).containsExactlyInAnyOrderElementsOf(diasNovos);
        assertThat(eventoPublicado.getDataAtualizacao()).isNotNull();
    }

    @Test
    @DisplayName("Deve atualizar e publicar apenas para a matrícula com dias diferentes (matrícula mista)")
    void deveAtualizarApenasMatriculaComDiasDiferentes() {
        // Arrange — uma matrícula com dias iguais, outra com dias diferentes
        List<String> diasNovos   = List.of("SEGUNDA", "QUARTA", "SEXTA");
        List<String> diasIguais  = List.of("SEGUNDA", "QUARTA", "SEXTA"); // já igual ao evento
        List<String> diasDiferentes = List.of("TERCA", "QUINTA");          // diferente do evento

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-001", diasNovos, "19:00", "22:00", 30);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-001", turmaEvento, 300L);

        Matricula matriculaSemAlteracao = matriculaComDias("MAT-002", "ALU-002", "BK-001", diasIguais);
        Matricula matriculaParaAtualizar = matriculaComDias("MAT-003", "ALU-003", "BK-001", diasDiferentes);
        Matricula matriculaAtualizada = matriculaComDias("MAT-003", "ALU-003", "BK-001", diasNovos);

        when(cicloClientPort.buscarPorId(300L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA))
                .thenReturn(List.of(matriculaSemAlteracao, matriculaParaAtualizar));
        when(matriculaRepositoryPort.salvar(any())).thenReturn(matriculaAtualizada);

        // Act
        service.processar(evento);

        // Assert — salvar chamado apenas uma vez (somente a que tinha dias diferentes)
        verify(matriculaRepositoryPort, times(1)).salvar(any());

        // Assert — publicar chamado apenas uma vez
        ArgumentCaptor<MatriculaAtualizadaEvent> captor = ArgumentCaptor.forClass(MatriculaAtualizadaEvent.class);
        verify(matriculaEventPublisherPort, times(1)).publicar(captor.capture());

        MatriculaAtualizadaEvent eventoPublicado = captor.getValue();
        assertThat(eventoPublicado.getMatriculaId()).isEqualTo("MAT-003");
        assertThat(eventoPublicado.getDiasDaSemanaAnterior()).containsExactlyInAnyOrderElementsOf(diasDiferentes);
        assertThat(eventoPublicado.getDiasDaSemanaNovo()).containsExactlyInAnyOrderElementsOf(diasNovos);
    }

    @Test
    @DisplayName("Deve atualizar e publicar para cada matrícula com dias diferentes (múltiplas matrículas)")
    void deveAtualizarEPublicarParaCadaMatriculaComDiasDiferentes() {
        // Arrange — duas matrículas, ambas com dias diferentes do evento
        List<String> diasNovos = List.of("SEGUNDA", "QUARTA", "SEXTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-001", diasNovos, "19:00", "22:00", 30);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-001", turmaEvento, 300L);

        Matricula matricula1 = matriculaComDias("MAT-010", "ALU-010", "BK-001", List.of("SEGUNDA"));
        Matricula matricula2 = matriculaComDias("MAT-011", "ALU-011", "BK-001", List.of("TERCA", "QUINTA"));

        when(cicloClientPort.buscarPorId(300L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-001", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula1, matricula2));
        when(matriculaRepositoryPort.salvar(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        // Act
        service.processar(evento);

        // Assert — salvar chamado para cada matrícula diferente
        verify(matriculaRepositoryPort, times(2)).salvar(any());

        // Assert — publicar chamado para cada matrícula diferente
        ArgumentCaptor<MatriculaAtualizadaEvent> captor = ArgumentCaptor.forClass(MatriculaAtualizadaEvent.class);
        verify(matriculaEventPublisherPort, times(2)).publicar(captor.capture());

        List<MatriculaAtualizadaEvent> eventosPublicados = captor.getAllValues();
        assertThat(eventosPublicados).hasSize(2);
        assertThat(eventosPublicados)
                .extracting(MatriculaAtualizadaEvent::getMatriculaId)
                .containsExactlyInAnyOrder("MAT-010", "MAT-011");
        assertThat(eventosPublicados)
                .allSatisfy(e -> assertThat(e.getDiasDaSemanaNovo())
                        .containsExactlyInAnyOrderElementsOf(diasNovos));
    }

    @Test
    @DisplayName("Deve preservar os dias anteriores corretamente no evento publicado")
    void devePreservarDiasAnterioresNoEventoPublicado() {
        // Arrange
        List<String> diasAntigos = List.of("SABADO", "DOMINGO");
        List<String> diasNovos   = List.of("SEGUNDA", "TERCA", "QUARTA");

        TurmaEventDTO turmaEvento = new TurmaEventDTO("TURMA-005", diasNovos, "07:00", "09:00", 20);
        TurmaAtualizadaEvent evento = new TurmaAtualizadaEvent("BK-005", turmaEvento, 300L);

        Matricula matricula = matriculaComDias("MAT-020", "ALU-020", "BK-005", diasAntigos);
        Matricula matriculaAtualizada = matriculaComDias("MAT-020", "ALU-020", "BK-005", diasNovos);

        when(cicloClientPort.buscarPorId(300L)).thenReturn(Optional.of(cicloVigente));
        when(matriculaRepositoryPort.buscarPorBusinessKeyEStatus("BK-005", StatusMatricula.ATIVA))
                .thenReturn(List.of(matricula));
        when(matriculaRepositoryPort.salvar(any())).thenReturn(matriculaAtualizada);

        // Act
        service.processar(evento);

        // Assert — dias anteriores não foram sobrescritos no evento publicado
        ArgumentCaptor<MatriculaAtualizadaEvent> captor = ArgumentCaptor.forClass(MatriculaAtualizadaEvent.class);
        verify(matriculaEventPublisherPort).publicar(captor.capture());

        MatriculaAtualizadaEvent eventoPublicado = captor.getValue();
        assertThat(eventoPublicado.getDiasDaSemanaAnterior())
                .containsExactlyInAnyOrderElementsOf(diasAntigos);
        assertThat(eventoPublicado.getDiasDaSemanaNovo())
                .containsExactlyInAnyOrderElementsOf(diasNovos);
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
                .cicloId(300L)
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

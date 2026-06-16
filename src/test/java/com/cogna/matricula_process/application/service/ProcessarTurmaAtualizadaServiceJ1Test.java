package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaEventDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * J1 — Ciclo não encontrado (404).
 *
 * Dado que o ciclo consultado retorna Optional.empty() (ex: API retornou 404),
 * o evento deve ser descartado silenciosamente:
 *  - Nenhuma matrícula deve ser buscada
 *  - Nenhuma matrícula deve ser salva
 *  - Nenhum evento deve ser publicado
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("J1 — Ciclo não encontrado: evento deve ser descartado")
class ProcessarTurmaAtualizadaServiceJ1Test {

    @Mock
    private CicloClientPort cicloClientPort;

    @Mock
    private MatriculaRepositoryPort matriculaRepositoryPort;

    @Mock
    private MatriculaEventPublisherPort matriculaEventPublisherPort;

    @InjectMocks
    private ProcessarTurmaAtualizadaService service;

    private TurmaAtualizadaEvent evento;

    @BeforeEach
    void setUp() {
        TurmaEventDTO turma = new TurmaEventDTO(
                "TURMA-001",
                List.of("SEGUNDA", "QUARTA"),
                "08:00",
                "10:00",
                30
        );

        evento = new TurmaAtualizadaEvent("BK-2024-001", turma, 99L);
    }

    @Test
    @DisplayName("Deve descartar o evento quando o ciclo não for encontrado (404)")
    void devDescartarEventoQuandoCicloNaoEncontrado() {
        // Arrange — API retorna vazio (404)
        when(cicloClientPort.buscarPorId(99L)).thenReturn(Optional.empty());

        // Act
        service.processar(evento);

        // Assert — ciclo foi consultado exatamente uma vez
        verify(cicloClientPort, times(1)).buscarPorId(99L);

        // Assert — repositório NUNCA foi acessado
        verify(matriculaRepositoryPort, never()).buscarPorBusinessKeyEStatus(any(), any());
        verify(matriculaRepositoryPort, never()).salvar(any());

        // Assert — nenhum evento foi publicado
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Deve descartar o evento independentemente do businessKey e cicloId recebidos")
    void devDescartarEventoParaQualquerCicloInexistente() {
        // Arrange — qualquer cicloId consultado retorna vazio
        when(cicloClientPort.buscarPorId(anyLong())).thenReturn(Optional.empty());

        TurmaEventDTO outraTurma = new TurmaEventDTO(
                "TURMA-999",
                List.of("TERCA", "QUINTA", "SABADO"),
                "19:00",
                "21:00",
                20
        );
        TurmaAtualizadaEvent outroEvento = new TurmaAtualizadaEvent("BK-9999", outraTurma, 999L);

        // Act
        service.processar(outroEvento);

        // Assert
        verify(cicloClientPort, times(1)).buscarPorId(999L);
        verifyNoInteractions(matriculaRepositoryPort);
        verifyNoInteractions(matriculaEventPublisherPort);
    }
}

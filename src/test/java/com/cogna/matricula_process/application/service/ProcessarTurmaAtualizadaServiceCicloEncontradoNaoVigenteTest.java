package com.cogna.matricula_process.application.service;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.application.port.out.MatriculaEventPublisherPort;
import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
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
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * C2 - Ciclo encontrado, mas não vigente.
 *
 * O ciclo é considerado NÃO vigente quando:
 *   - ativo == false, OU
 *   - a data atual está fora da janela de captura (dataInicioCaptura <= hoje < dataFimCaptura)
 *
 * Em qualquer desses casos o evento deve ser descartado:
 *   - Nenhuma matrícula deve ser buscada
 *   - Nenhuma matrícula deve ser salva
 *   - Nenhum evento deve ser publicado
 */
@ExtendWith(MockitoExtension.class)
@DisplayName(" C2 - Ciclo não vigente: evento deve ser descartado")
class ProcessarTurmaAtualizadaServiceCicloEncontradoNaoVigenteTest {

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
        evento = new TurmaAtualizadaEvent("BK-2024-001", turma, 100L);
    }

    @Test
    @DisplayName("Deve descartar o evento quando o ciclo está inativo (ativo=false)")
    void deveDescartarEventoQuandoCicloInativo() {
        // Arrange — ciclo encontrado, mas ativo=false
        CicloResponseDTO cicloInativo = new CicloResponseDTO(
                100L,
                false,
                LocalDate.now().minusDays(10),
                LocalDate.now().plusDays(10)
        );
        when(cicloClientPort.buscarPorId(100L)).thenReturn(Optional.of(cicloInativo));

        // Act
        service.processar(evento);

        // Assert — ciclo foi consultado
        verify(cicloClientPort, times(1)).buscarPorId(100L);

        // Assert — repositório NUNCA foi acessado
        verify(matriculaRepositoryPort, never()).buscarPorBusinessKeyEStatus(any(), any());
        verify(matriculaRepositoryPort, never()).salvar(any());

        // Assert — nenhum evento foi publicado
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Deve descartar o evento quando a data atual é anterior ao início da captura")
    void deveDescartarEventoQuandoForaDoInicioCaptura() {
        // Arrange — ciclo ativo, mas captura ainda não começou
        CicloResponseDTO cicloFuturo = new CicloResponseDTO(
                100L,
                true,
                LocalDate.now().plusDays(1),   // início no futuro
                LocalDate.now().plusDays(30)
        );
        when(cicloClientPort.buscarPorId(100L)).thenReturn(Optional.of(cicloFuturo));

        // Act
        service.processar(evento);

        // Assert
        verify(cicloClientPort, times(1)).buscarPorId(100L);
        verify(matriculaRepositoryPort, never()).buscarPorBusinessKeyEStatus(any(), any());
        verify(matriculaRepositoryPort, never()).salvar(any());
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Deve descartar o evento quando a data atual é igual ou posterior ao fim da captura")
    void deveDescartarEventoQuandoForaDoFimCaptura() {
        // Arrange — ciclo ativo, mas janela de captura já encerrou
        CicloResponseDTO cicloEncerrado = new CicloResponseDTO(
                100L,
                true,
                LocalDate.now().minusDays(30),
                LocalDate.now()   // dataFimCaptura == hoje → hoje NÃO está dentro da janela (exclusivo)
        );
        when(cicloClientPort.buscarPorId(100L)).thenReturn(Optional.of(cicloEncerrado));

        // Act
        service.processar(evento);

        // Assert
        verify(cicloClientPort, times(1)).buscarPorId(100L);
        verify(matriculaRepositoryPort, never()).buscarPorBusinessKeyEStatus(any(), any());
        verify(matriculaRepositoryPort, never()).salvar(any());
        verify(matriculaEventPublisherPort, never()).publicar(any());
    }

    @Test
    @DisplayName("Deve descartar o evento quando ciclo inativo E fora da janela de captura")
    void deveDescartarEventoQuandoCicloInativoEForaDaJanela() {
        // Arrange — pior caso: inativo e fora da janela
        CicloResponseDTO cicloInativoFora = new CicloResponseDTO(
                100L,
                false,
                LocalDate.now().minusDays(60),
                LocalDate.now().minusDays(30)
        );
        when(cicloClientPort.buscarPorId(100L)).thenReturn(Optional.of(cicloInativoFora));

        // Act
        service.processar(evento);

        // Assert
        verify(cicloClientPort, times(1)).buscarPorId(100L);
        verifyNoInteractions(matriculaRepositoryPort);
        verifyNoInteractions(matriculaEventPublisherPort);
    }
}

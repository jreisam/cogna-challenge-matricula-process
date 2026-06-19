package com.cogna.matricula_process.infrastructure.adapter.in.rest;

import com.cogna.matricula_process.application.port.in.ProcessarTurmaAtualizadaUseCase;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaEventDTO;
import com.cogna.matricula_process.infrastructure.adapter.in.rest.dto.ProcessamentoResponse;
import com.cogna.matricula_process.infrastructure.adapter.in.rest.dto.TurmaAtualizadaRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/turma-atualizada")
@RequiredArgsConstructor
public class TurmaAtualizadaRestController {

    private final ProcessarTurmaAtualizadaUseCase processarTurmaAtualizadaUseCase;

    /**
     * Endpoint para disparar o processamento de turma atualizada via REST,
     * permitindo testar e validar os cenários sem depender de eventos Kafka.
     *
     * <p>Cenários testáveis:
     * <ul>
     *   <li>Ciclo não encontrado (404 na API de ciclos)</li>
     *   <li>Ciclo encontrado porém não vigente (inativo ou fora da janela de captura)</li>
     *   <li>Ciclo vigente, dias da semana iguais → nenhuma atualização</li>
     *   <li>Ciclo vigente, dias da semana diferentes → matrículas atualizadas e eventos publicados</li>
     * </ul>
     *
     * @param request payload equivalente ao evento Kafka {@code turma-atualizada}
     * @return 200 OK com informações do processamento realizado
     */
    @PostMapping("/processar")
    public ResponseEntity<ProcessamentoResponse> processar(@Valid @RequestBody TurmaAtualizadaRequest request) {
        log.info("Recebida requisição REST para processar turma atualizada | businessKey={} | cicloId={}",
                request.getBusinessKey(), request.getCicloId());

                        TurmaEventDTO turmaEventDTO = new TurmaEventDTO(
                null,
                request.getDiasDaSemana(),
                request.getHorarioInicio(),
                request.getHorarioFim(),
                null
                        );

                        TurmaAtualizadaEvent event = TurmaAtualizadaEvent.builder()
                .businessKey(request.getBusinessKey())
                .cicloId(request.getCicloId())
                .turma(turmaEventDTO)
                .build();

        processarTurmaAtualizadaUseCase.processar(event);

        ProcessamentoResponse response = ProcessamentoResponse.builder()
                .status("PROCESSADO")
                .mensagem("Evento de turma atualizada processado com sucesso via REST.")
                .businessKey(request.getBusinessKey())
                .cicloId(request.getCicloId())
                .build();

        log.info("Processamento REST concluído | businessKey={} | cicloId={}",
                request.getBusinessKey(), request.getCicloId());

        return ResponseEntity.ok(response);
    }
}

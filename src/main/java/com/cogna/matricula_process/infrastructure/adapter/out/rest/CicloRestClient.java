package com.cogna.matricula_process.infrastructure.adapter.out.rest;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CicloRestClient implements CicloClientPort {

    private final RestClient cicloRestClient;

    @Override
    public Optional<CicloResponse> buscarPorId(Long cicloId) {
        log.info("Consultando ciclo id={} na API externa", cicloId);
        try {
            CicloResponse response = cicloRestClient
                    .get()
                    .uri("/api/ciclos/{cicloId}", cicloId)
                    .retrieve()
                    .body(CicloResponse.class);

            log.info("Ciclo id={} encontrado: ativo={}", cicloId, response != null && response.isAtivo());
            return Optional.ofNullable(response);

        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Ciclo id={} não encontrado (404). Evento será descartado.", cicloId);
            return Optional.empty();
        }
    }
}

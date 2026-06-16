package com.cogna.matricula_process.infrastructure.adapter.out.rest;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CicloRestAdapter implements CicloClientPort {

    private final RestClient cicloRestClient;

    @Override
    public Optional<CicloResponseDTO> buscarPorId(Long cicloId) {
        log.info("Consultando ciclo id={} na API externa", cicloId);

        return cicloRestClient.get()
                .uri("/api/ciclos/{cicloId}", cicloId)
                .retrieve()
                .onStatus(HttpStatusCode::is4xxClientError, (request, response) -> {
                    log.warn("Ciclo id={} não encontrado na API externa (status {})", cicloId, response.getStatusCode());
                })
                .toEntity(CicloResponseDTO.class)
                .getBody() != null
                ? Optional.ofNullable(
                        cicloRestClient.get()
                                .uri("/api/ciclos/{cicloId}", cicloId)
                                .retrieve()
                                .onStatus(HttpStatusCode::is4xxClientError, (request, response) -> {})
                                .toEntity(CicloResponseDTO.class)
                                .getBody()
                  )
                : Optional.empty();
    }
}

package com.cogna.matricula_process.infrastructure.adapter.out.rest;

import com.cogna.matricula_process.application.port.out.CicloClientPort;
import com.cogna.matricula_process.infrastructure.adapter.out.rest.dto.CicloResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
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
        try {
            String url = "/api/ciclos/" + cicloId;
            var response = cicloRestClient.get().uri(url).accept(MediaType.APPLICATION_JSON).retrieve().toEntity(CicloResponseDTO.class);

            if (response == null || !HttpStatusCode.valueOf(response. getStatusCodeValue()).is2xxSuccessful()) {
                log.error("Erro ao consultar ciclo id={} | StatusCode={}", cicloId, response != null ? response.getStatusCodeValue() : "null");
                return Optional.empty();
            }

            return Optional.ofNullable(response.getBody());
        } catch (Exception ex) {
            log.error("Exceção ao consultar ciclo id={}: {}", cicloId, ex.getMessage(), ex);
            return Optional.empty();
        }

    }
}

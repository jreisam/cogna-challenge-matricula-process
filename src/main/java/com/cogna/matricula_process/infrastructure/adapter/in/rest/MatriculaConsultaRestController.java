package com.cogna.matricula_process.infrastructure.adapter.in.rest;

import com.cogna.matricula_process.application.port.out.MatriculaRepositoryPort;
import com.cogna.matricula_process.domain.model.Matricula;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class MatriculaConsultaRestController {

    private final MatriculaRepositoryPort matriculaRepositoryPort;

    /**
     * Consulta matrículas ATIVAS por businessKey.
     * Permite validar o estado das matrículas antes e após um processamento.
     *
     * Exemplo: GET /api/test/matriculas?businessKey=TURMA-2024-001
     */
    @GetMapping("/matriculas")
    public ResponseEntity<List<Matricula>> buscarMatriculasAtivas(
            @RequestParam String businessKey) {

        log.info("[REST] Consultando matrículas ATIVAS | businessKey={}", businessKey);

        List<Matricula> matriculas = matriculaRepositoryPort.buscarAtivasPorBusinessKey(businessKey);

        log.info("[REST] Encontradas {} matrículas | businessKey={}", matriculas.size(), businessKey);

        return ResponseEntity.ok(matriculas);
    }
}

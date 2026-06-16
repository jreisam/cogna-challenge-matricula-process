package com.cogna.matricula_process.infrastructure.adapter.in.kafka;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * Utilitário para gerenciar o contexto de rastreabilidade via MDC (Mapped Diagnostic Context).
 *
 * <p>Todos os campos populados aqui aparecem automaticamente em cada linha de log JSON
 * gerada dentro da mesma thread, permitindo filtrar/correlacionar logs por evento
 * sem alterar as assinaturas dos métodos internos.
 *
 * <p>Uso padrão:
 * <pre>{@code
 *   MdcContext.popular(event);
 *   try {
 *       // processamento...
 *   } finally {
 *       MdcContext.limpar();
 *   }
 * }</pre>
 */
public final class MdcContext {

    public static final String CORRELATION_ID = "correlationId";
    public static final String BUSINESS_KEY   = "businessKey";
    public static final String CICLO_ID       = "cicloId";
    public static final String MATRICULA_ID   = "matriculaId";
    public static final String ALUNO_ID       = "alunoId";

    private MdcContext() {}

    /**
     * Popula o MDC com os campos do evento recebido do Kafka.
     * Gera um {@code correlationId} único por execução para rastrear
     * todos os logs de um mesmo processamento de ponta a ponta.
     */
    public static void popular(String businessKey, Long cicloId) {
        MDC.put(CORRELATION_ID, UUID.randomUUID().toString());
        MDC.put(BUSINESS_KEY, businessKey != null ? businessKey : "N/A");
        MDC.put(CICLO_ID, cicloId != null ? cicloId.toString() : "N/A");
    }

    /** Enriquece o MDC com dados de uma matrícula específica durante o loop de processamento. */
    public static void enriquecerComMatricula(String matriculaId, String alunoId) {
        MDC.put(MATRICULA_ID, matriculaId != null ? matriculaId : "N/A");
        MDC.put(ALUNO_ID, alunoId != null ? alunoId : "N/A");
    }

    /** Remove todos os campos do MDC — SEMPRE deve ser chamado no bloco {@code finally}. */
    public static void limpar() {
        MDC.remove(CORRELATION_ID);
        MDC.remove(BUSINESS_KEY);
        MDC.remove(CICLO_ID);
        MDC.remove(MATRICULA_ID);
        MDC.remove(ALUNO_ID);
    }
}

package com.cogna.matricula_process.infrastructure.config;

import org.slf4j.MDC;

import java.util.Map;

/**
 * Utilitário para gerenciar chaves MDC de forma segura usando try-with-resources.
 *
 * <p>Uso:
 * <pre>{@code
 * try (MdcContext ctx = MdcContext.of("correlationId", id, "businessKey", bk)) {
 *     // todos os logs dentro deste bloco terão os campos MDC preenchidos
 * }
 * // MDC limpo automaticamente ao sair do bloco
 * }</pre>
 */
public class MdcContext implements AutoCloseable {

    // Chaves MDC padronizadas para o projeto
    public static final String CORRELATION_ID = "correlationId";
    public static final String BUSINESS_KEY   = "businessKey";
    public static final String CICLO_ID       = "cicloId";
    public static final String MATRICULA_ID   = "matriculaId";
    public static final String ALUNO_ID       = "alunoId";

    private final Map<String, String> entries;

    private MdcContext(Map<String, String> entries) {
        this.entries = entries;
        entries.forEach(MDC::put);
    }

    /**
     * Cria e popula o MDC com os pares chave/valor fornecidos.
     * Valores nulos são convertidos para a string "null" para evitar erros no encoder.
     *
     * @param keyValuePairs alternância de chave, valor, chave, valor...
     */
    public static MdcContext of(String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("keyValuePairs deve conter um número par de elementos");
        }
        var map = new java.util.LinkedHashMap<String, String>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put(keyValuePairs[i], keyValuePairs[i + 1] != null ? keyValuePairs[i + 1] : "null");
        }
        return new MdcContext(map);
    }

    /**
     * Adiciona ou atualiza uma chave no MDC e neste contexto.
     */
    public MdcContext put(String key, String value) {
        entries.put(key, value != null ? value : "null");
        MDC.put(key, entries.get(key));
        return this;
    }

    @Override
    public void close() {
        entries.keySet().forEach(MDC::remove);
    }
}

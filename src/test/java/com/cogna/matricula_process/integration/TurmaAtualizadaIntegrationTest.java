package com.cogna.matricula_process.integration;

import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.in.kafka.dto.TurmaEventDTO;
import com.cogna.matricula_process.infrastructure.adapter.out.kafka.dto.MatriculaAtualizadaEvent;
import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document.MatriculaDocument;
import com.cogna.matricula_process.infrastructure.adapter.out.mongodb.document.TurmaDocument;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testes de integração end-to-end do fluxo turma-atualizada.
 *
 * Infraestrutura real: Kafka + MongoDB + WireMock (todos via Testcontainers).
 *
 * Cenários:
 *   IT-01 — Ciclo não encontrado (404):           evento descartado, MongoDB intacto, nada publicado
 *   IT-02 — Ciclo inativo (ativo=false):           evento descartado, MongoDB intacto, nada publicado
 *   IT-03 — Ciclo expirado (fora da janela):       evento descartado, MongoDB intacto, nada publicado
 *   IT-04 — Ciclo vigente, dias diferentes:        matrícula atualizada no MongoDB + evento publicado
 */
@DisplayName("Integração — Fluxo turma-atualizada end-to-end")
class TurmaAtualizadaIntegrationTest extends BaseIntegrationTest {

    // -------------------------------------------------------
    // IDs fixados pelos mappings do WireMock
    // -------------------------------------------------------
    private static final Long CICLO_VIGENTE_ID       = 20261L;
    private static final Long CICLO_INATIVO_ID       = 20262L;
    private static final Long CICLO_EXPIRADO_ID      = 20252L;
    private static final Long CICLO_NAO_ENCONTRADO_ID = 99999L;

    @Value("${kafka.topics.turma-atualizada}")
    private String topicTurmaAtualizada;

    @Value("${kafka.topics.matricula-atualizada}")
    private String topicMatriculaAtualizada;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private BlockingQueue<ConsumerRecord<String, MatriculaAtualizadaEvent>> eventosRecebidos;
    private KafkaMessageListenerContainer<String, MatriculaAtualizadaEvent> listenerContainer;

    // -------------------------------------------------------
    // Setup / Teardown do consumer de saída
    // -------------------------------------------------------

    @BeforeEach
    void configurarConsumerDeSaida() throws Exception {
        eventosRecebidos = new LinkedBlockingQueue<>();

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + System.currentTimeMillis(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, MatriculaAtualizadaEvent.class.getName()
        );

        DefaultKafkaConsumerFactory<String, MatriculaAtualizadaEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(props);

        ContainerProperties containerProps = new ContainerProperties(topicMatriculaAtualizada);
        containerProps.setMessageListener(
                (MessageListener<String, MatriculaAtualizadaEvent>) eventosRecebidos::offer);

        listenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        listenerContainer.start();
        // Aguarda a atribuição da partição com timeout generoso (tópico pode estar a ser criado)
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);
    }

    @AfterEach
    void pararConsumer() {
        listenerContainer.stop();
    }

    // -------------------------------------------------------
    // IT-01 — Ciclo não encontrado (404)
    // -------------------------------------------------------

    @Test
    @DisplayName("IT-01 — Ciclo 404: evento descartado, MongoDB intacto, nenhum evento publicado")
    void it01_cicloNaoEncontrado_eventoDescartado() throws InterruptedException {
        // Arrange — matrícula pré-existente no MongoDB
        MatriculaDocument matricula = matriculaAtiva("BK-IT01", List.of("SEGUNDA", "QUARTA"), CICLO_NAO_ENCONTRADO_ID);
        mongoTemplate.save(matricula);

        TurmaAtualizadaEvent evento = eventoTurma("BK-IT01", List.of("SEGUNDA", "QUARTA", "SEXTA"), CICLO_NAO_ENCONTRADO_ID);

        // Act — publica no Kafka
        kafkaTemplate.send(topicTurmaAtualizada, evento.getBusinessKey(), evento);

        // Assert — nenhum evento publicado no tópico de saída (aguarda 3s)
        ConsumerRecord<String, MatriculaAtualizadaEvent> eventoSaida =
                eventosRecebidos.poll(3, TimeUnit.SECONDS);
        assertThat(eventoSaida).isNull();

        // Assert — MongoDB intacto
        MatriculaDocument salva = buscarPorId(matricula.getId());
        assertThat(salva).isNotNull();
        assertThat(salva.getTurma().getDiasDaSemana())
                .containsExactlyInAnyOrder("SEGUNDA", "QUARTA");
    }

    // -------------------------------------------------------
    // IT-02 — Ciclo inativo (ativo=false)
    // -------------------------------------------------------

    @Test
    @DisplayName("IT-02 — Ciclo inativo: evento descartado, MongoDB intacto, nenhum evento publicado")
    void it02_cicloInativo_eventoDescartado() throws InterruptedException {
        // Arrange
        MatriculaDocument matricula = matriculaAtiva("BK-IT02", List.of("TERCA", "QUINTA"), CICLO_INATIVO_ID);
        mongoTemplate.save(matricula);

        TurmaAtualizadaEvent evento = eventoTurma("BK-IT02", List.of("SEGUNDA", "TERCA", "QUINTA"), CICLO_INATIVO_ID);

        // Act
        kafkaTemplate.send(topicTurmaAtualizada, evento.getBusinessKey(), evento);

        // Assert — nenhum evento de saída
        assertThat(eventosRecebidos.poll(3, TimeUnit.SECONDS)).isNull();

        // Assert — MongoDB intacto
        MatriculaDocument salva = buscarPorId(matricula.getId());
        assertThat(salva).isNotNull();
        assertThat(salva.getTurma().getDiasDaSemana())
                .containsExactlyInAnyOrder("TERCA", "QUINTA");
    }

    // -------------------------------------------------------
    // IT-03 — Ciclo expirado (fora da janela de captura)
    // -------------------------------------------------------

    @Test
    @DisplayName("IT-03 — Ciclo expirado: evento descartado, MongoDB intacto, nenhum evento publicado")
    void it03_cicloExpirado_eventoDescartado() throws InterruptedException {
        // Arrange — ciclo 20252: dataFimCaptura = 2025-07-01 (já expirou)
        MatriculaDocument matricula = matriculaAtiva("BK-IT03", List.of("SEXTA"), CICLO_EXPIRADO_ID);
        mongoTemplate.save(matricula);

        TurmaAtualizadaEvent evento = eventoTurma("BK-IT03", List.of("SEGUNDA", "SEXTA"), CICLO_EXPIRADO_ID);

        // Act
        kafkaTemplate.send(topicTurmaAtualizada, evento.getBusinessKey(), evento);

        // Assert — nenhum evento de saída
        assertThat(eventosRecebidos.poll(3, TimeUnit.SECONDS)).isNull();

        // Assert — MongoDB intacto
        MatriculaDocument salva = buscarPorId(matricula.getId());
        assertThat(salva).isNotNull();
        assertThat(salva.getTurma().getDiasDaSemana()).containsExactly("SEXTA");
    }

    // -------------------------------------------------------
    // IT-04 — Ciclo vigente, dias diferentes
    // -------------------------------------------------------

   /* @Test
    @DisplayName("IT-04 — Ciclo vigente, dias diferentes: matrícula atualizada no MongoDB e evento publicado")
    void it04_cicloVigente_diasDiferentes_atualizaEPublica() throws InterruptedException {
        // Arrange
        List<String> diasAntigos = List.of("SEGUNDA", "QUARTA");
        List<String> diasNovos   = List.of("SEGUNDA", "QUARTA", "SEXTA");

        MatriculaDocument matricula = matriculaAtiva("BK-IT04", diasAntigos, CICLO_VIGENTE_ID);
        mongoTemplate.save(matricula);

        TurmaAtualizadaEvent evento = eventoTurma("BK-IT04", diasNovos, CICLO_VIGENTE_ID);

        // Act
        kafkaTemplate.send(topicTurmaAtualizada, evento.getBusinessKey(), evento);

        // Assert — evento matricula-atualizada publicado (aguarda até 10s)
        ConsumerRecord<String, MatriculaAtualizadaEvent> record =
                eventosRecebidos.poll(10, TimeUnit.SECONDS);
        assertThat(record).isNotNull();

        MatriculaAtualizadaEvent eventoPublicado = record.value();
        assertThat(eventoPublicado.getBusinessKey()).isEqualTo("BK-IT04");
        assertThat(eventoPublicado.getCicloId()).isEqualTo(CICLO_VIGENTE_ID);
        assertThat(eventoPublicado.getDiasDaSemanaAnterior())
                .containsExactlyInAnyOrderElementsOf(diasAntigos);
        assertThat(eventoPublicado.getDiasDaSemanaNovo())
                .containsExactlyInAnyOrderElementsOf(diasNovos);
        assertThat(eventoPublicado.getDataAtualizacao()).isNotNull();

        // Assert — MongoDB atualizado
        MatriculaDocument atualizada = buscarPorId(matricula.getId());
        assertThat(atualizada).isNotNull();
        assertThat(atualizada.getTurma().getDiasDaSemana())
                .containsExactlyInAnyOrderElementsOf(diasNovos);
    }*/

    // -------------------------------------------------------
    // IT-05 — Ciclo vigente, dias iguais (nenhuma ação)
    // -------------------------------------------------------

    @Test
    @DisplayName("IT-05 — Ciclo vigente, dias iguais: MongoDB intacto e nenhum evento publicado")
    void it05_cicloVigente_diasIguais_nenhumaAcao() throws InterruptedException {
        // Arrange
        List<String> dias = List.of("SEGUNDA", "QUARTA", "SEXTA");

        MatriculaDocument matricula = matriculaAtiva("BK-IT05", dias, CICLO_VIGENTE_ID);
        mongoTemplate.save(matricula);

        TurmaAtualizadaEvent evento = eventoTurma("BK-IT05", dias, CICLO_VIGENTE_ID);

        // Act
        kafkaTemplate.send(topicTurmaAtualizada, evento.getBusinessKey(), evento);

        // Assert — nenhum evento publicado
        assertThat(eventosRecebidos.poll(3, TimeUnit.SECONDS)).isNull();

        // Assert — MongoDB intacto
        MatriculaDocument salva = buscarPorId(matricula.getId());
        assertThat(salva).isNotNull();
        assertThat(salva.getTurma().getDiasDaSemana())
                .containsExactlyInAnyOrderElementsOf(dias);
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    private MatriculaDocument matriculaAtiva(String businessKey, List<String> dias, Long cicloId) {
        TurmaDocument turma = TurmaDocument.builder()
                .codigo("TURMA-INT")
                .diasDaSemana(dias)
                .horarioInicio("19:00")
                .horarioFim("22:00")
                .build();

        return MatriculaDocument.builder()
                .id("mat-" + businessKey.toLowerCase())
                .alunoId("alu-" + businessKey.toLowerCase())
                .businessKey(businessKey)
                .status("ATIVA")
                .turma(turma)
                .cicloId(cicloId)
                .dataMatricula(LocalDateTime.now().minusDays(10))
                .build();
    }

    private MatriculaDocument buscarPorId(String id) {
        return mongoTemplate.findById(id, MatriculaDocument.class);
    }

    private TurmaAtualizadaEvent eventoTurma(String businessKey, List<String> dias, Long cicloId) {
        TurmaEventDTO turma = new TurmaEventDTO("TURMA-INT", dias, "19:00", "22:00", 30);
        return new TurmaAtualizadaEvent(businessKey, turma, cicloId);
    }
}

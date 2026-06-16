package com.cogna.matricula_process.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.wiremock.integrations.testcontainers.WireMockContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
public abstract class BaseIntegrationTest {

    // -------------------------------------------------------
    // Containers — compartilhados entre todos os testes
    // -------------------------------------------------------

    @Container
    @ServiceConnection
    static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));

    @Container
    @ServiceConnection
    static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer(DockerImageName.parse("mongo:latest"));

    @Container
    static final WireMockContainer wireMockContainer =
            new WireMockContainer(DockerImageName.parse("wiremock/wiremock:3.9.1"))
                    .withMappingFromResource("ciclo-vigente",
                            "wiremock/mappings/ciclo-vigente.json")
                    .withMappingFromResource("ciclo-inativo",
                            "wiremock/mappings/ciclo-inativo.json")
                    .withMappingFromResource("ciclo-expirado",
                            "wiremock/mappings/ciclo-expirado.json")
                    .withMappingFromResource("ciclo-nao-encontrado",
                            "wiremock/mappings/ciclo-nao-encontrado.json");

    // -------------------------------------------------------
    // Override dinâmico da URL do ciclo para o WireMock
    // -------------------------------------------------------

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("ciclo.api.url", wireMockContainer::getBaseUrl);
    }

    // -------------------------------------------------------
    // Garante que os tópicos existam uma única vez antes de todos os testes
    // -------------------------------------------------------

    @BeforeAll
    static void criarTopicos() throws Exception {
        Map<String, Object> props = Map.of(
                "bootstrap.servers", kafkaContainer.getBootstrapServers()
        );
        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> topicos = List.of(
                    new NewTopic("turma-atualizada", 1, (short) 1),
                    new NewTopic("matricula-atualizada", 1, (short) 1),
                    new NewTopic("turma-atualizada.DLT", 1, (short) 1)
            );
            try {
                adminClient.createTopics(topicos).all().get();
            } catch (ExecutionException e) {
                // Ignora se os tópicos já existirem (ex: segunda classe de teste)
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
        }
    }

    // -------------------------------------------------------
    // Beans injetados para uso nos testes filhos
    // -------------------------------------------------------

    @Autowired
    protected KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    protected MongoTemplate mongoTemplate;

    // -------------------------------------------------------
    // Limpeza do MongoDB entre testes
    // -------------------------------------------------------

    @BeforeEach
    void limparMongoDB() {
        mongoTemplate.getDb().drop();
    }
}

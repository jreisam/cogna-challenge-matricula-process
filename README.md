# 📚 matricula-process

Microsserviço responsável por processar eventos de atualização de turmas e refletir as mudanças nas matrículas ativas dos alunos.

---

## 📖 Sobre a aplicação

Quando uma turma tem seus dias da semana alterados, o serviço `matricula-process` consome o evento `turma-atualizada` via Kafka e executa as seguintes regras de negócio:

1. **Valida o ciclo** — consulta a API de Ciclos para verificar se o ciclo informado existe e está vigente (ativo e dentro da janela de captura).
2. **Busca as matrículas ATIVAS** — localiza no MongoDB todas as matrículas vinculadas à `businessKey` da turma com status `ATIVA`.
3. **Compara os dias da semana** — para cada matrícula encontrada, verifica se os dias atuais diferem dos novos dias recebidos no evento.
4. **Atualiza e publica** — para cada matrícula com dias diferentes, persiste a alteração no MongoDB e publica um evento `matricula-atualizada` no Kafka com os dados antes e depois da mudança.

---

## 🏛️ Arquitetura

O projeto segue os princípios da **Arquitetura Hexagonal (Ports & Adapters)**, mantendo o domínio e as regras de negócio isolados de frameworks e infraestrutura.
| Camada | Responsabilidade |
|---|---|
| `domain` | Modelos e exceções de negócio |
| `application/port` | Interfaces (contratos) de entrada e saída |
| `application/service` | Regras de negócio |
| `infrastructure/adapter/in` | Consumer Kafka (entrada de eventos) |
| `infrastructure/adapter/out` | MongoDB, Kafka Producer e REST Client |

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Uso |
|---|---|---|
| **Java** | 21 | Linguagem principal |
| **Spring Boot** | 3.5.x | Framework base |
| **Spring Data MongoDB** | — | Persistência de matrículas |
| **Spring Kafka** | — | Consumo e publicação de eventos |
| **Spring Web / RestClient** | — | Integração com a API de Ciclos |
| **Spring Actuator** | — | Health check e métricas |
| **Lombok** | — | Redução de boilerplate |
| **MongoDB** | 7.0 | Banco de dados de matrículas |
| **Apache Kafka** | — | Mensageria assíncrona |
| **WireMock** | — | Mock da API de Ciclos (ambiente local) |
| **JUnit 5** | — | Framework de testes |
| **Mockito** | — | Mocks nos testes de unidade |
| **Testcontainers** | — | Testes de integração com Kafka e MongoDB reais em containers |
| **Docker / Docker Compose** | — | Orquestração do ambiente local |

---

## 📨 Tópicos Kafka

| Tópico | Direção | Descrição |
|---|---|---|
| `turma-atualizada` | **Entrada** (Consumer) | Evento recebido quando uma turma tem seus dados alterados |
| `matricula-atualizada` | **Saída** (Producer) | Evento publicado após atualização bem-sucedida de uma matrícula |

---

## ⚙️ Variáveis de ambiente

| Variável | Padrão (local) | Descrição |
|---|---|---|
| `MONGODB_URI` | `mongodb://localhost:27017/matriculas` | URI de conexão com o MongoDB |
| `MONGODB_DATABASE` | `matriculas` | Nome do banco de dados |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Endereço do broker Kafka |
| `KAFKA_CONSUMER_GROUP_ID` | `matricula-process-group` | Consumer group do Kafka |
| `KAFKA_TOPIC_TURMA_ATUALIZADA` | `turma-atualizada` | Nome do tópico de entrada |
| `KAFKA_TOPIC_MATRICULA_ATUALIZADA` | `matricula-atualizada` | Nome do tópico de saída |
| `CICLO_API_URL` | `http://localhost:8081` | URL base da API de Ciclos |

---

## 🚀 Executando localmente

### Pré-requisitos

- Docker e Docker Compose

### ▶️ Subindo tudo com um único comando

`docker compose up --build`
ou: `docker compose up --build -d` (detached)


Este comando irá:
1. **Buildar** a imagem da aplicação a partir do `Dockerfile` (multi-stage com Java 21)
2. **Inicializar** todos os serviços na ordem correta:

| Container | Porta | Descrição |
|---|---|---|
| `matricula-process` | `8080` | A aplicação Spring Boot |
| `matricula-mongodb` | `27017` | MongoDB com seed de dados iniciais |
| `matricula-kafka` | `9092` | Apache Kafka |
| `matricula-zookeeper` | `2181` | Zookeeper (dependência do Kafka) |
| `mock-ciclo-api` | `8081` | Mock WireMock da API de Ciclos |

> 💡 A aplicação aguarda automaticamente o MongoDB, o Kafka e o WireMock estarem saudáveis antes de iniciar (`depends_on` com `condition: service_healthy`).


---

## 🧪 Executando os testes

Os testes **não dependem do Docker Compose**. Eles usam **Testcontainers** para subir MongoDB e Kafka automaticamente durante a execução.

**Pré-requisito:** Java 21+ e Docker em execução.

# Todos os testes (unitários + integração com Testcontainers)
`./gradlew test`

---

## 🧪 Estratégia de testes

| Tipo | Ferramenta | Escopo |
|---|---|---|
| **Unidade** | JUnit 5 + Mockito | Regras de negócio do `Service` isoladas, com todas as portas mockadas |
| **Integração** | Testcontainers + Spring Boot Test | Jornadas completas com MongoDB e Kafka em containers Docker |




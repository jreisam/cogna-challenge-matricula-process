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
Todas as portas de saída (`CicloClientPort`, `MatriculaRepositoryPort`, `MatriculaEventPublisherPort`) são mockadas, isolando as regras de negócio do `ProcessarTurmaAtualizadaService`.

**Pré-requisito:** Java 21+ e Docker em execução.

# Todos os testes unitários
`./gradlew test --info`

---

## 🧪 Estratégia de testes

| Tipo | Ferramenta | Escopo |
|---|---|---|
| **Unidade** | JUnit 5 + Mockito | Regras de negócio do `Service` isoladas, com todas as portas mockadas |
| **Integração** | Testcontainers + Spring Boot Test | Jornadas completas com MongoDB e Kafka em containers Docker |


---

### 📋 Cenários cobertos

#### `ProcessarTurmaAtualizadaServiceCicloNaoEncontradoTest`
> Ciclo retorna `Optional.empty()` (ex: API respondeu 404)

| # | Cenário | Resultado esperado |
|---|---|---|
| 1 | Ciclo não encontrado para o `cicloId` do evento | Evento descartado — nenhuma matrícula buscada, salva ou publicada |
| 2 | Qualquer `cicloId` inexistente (parametrizado) | Evento descartado — nenhuma interação com repositório ou publisher |

---

#### `ProcessarTurmaAtualizadaServiceCicloEncontradoNaoVigenteTest`
> Ciclo encontrado, mas não satisfaz as condições de vigência (`ativo == true` e `dataInicioCaptura` ≤ hoje < `dataFimCaptura`)

| # | Cenário | Resultado esperado |
|---|---|---|
| 1 | `ativo = false`, janela de captura válida | Evento descartado |
| 2 | `ativo = true`, `hoje < dataInicioCaptura` (captura ainda não começou) | Evento descartado |
| 3 | `ativo = true`, `hoje >= dataFimCaptura` (limite superior exclusivo) | Evento descartado |
| 4 | `ativo = false` e fora da janela de captura | Evento descartado |

---

#### `ProcessarTurmaAtualizadaServiceCicloVigenteDiasIguaisTest`
> Ciclo vigente, mas `diasDaSemana` da matrícula já são idênticos aos do evento

| # | Cenário | Resultado esperado |
|---|---|---|
| 1 | Uma matrícula com dias iguais na mesma ordem | Nada é salvo ou publicado |
| 2 | Uma matrícula com dias iguais em ordem diferente | Nada é salvo ou publicado (comparação ignora ordem) |
| 3 | Múltiplas matrículas, todas com dias iguais ao evento | Nenhuma é salva ou publicada |
| 4 | Nenhuma matrícula `ATIVA` encontrada para o `businessKey` | Nenhuma ação realizada |

---

#### `ProcessarTurmaAtualizadaServiceCicloVigenteDiasDiferentesTest`
> Ciclo vigente e `diasDaSemana` da matrícula diferem dos dias do evento → deve persistir e publicar

| # | Cenário | Resultado esperado |
|---|---|---|
| 1 | Uma matrícula com dias diferentes | Matrícula salva + evento `matricula-atualizada` publicado com dados antes e depois |
| 2 | Duas matrículas: uma com dias iguais, outra com dias diferentes | Apenas a diferente é salva e publica evento |
| 3 | Múltiplas matrículas, todas com dias diferentes | `salvar` e `publicar` chamados N vezes (uma vez por matrícula) |
| 4 | Validação dos campos do evento publicado (`diasDaSemanaAnterior` preservado) | `diasDaSemanaAnterior` contém os dias originais da matrícula; `diasDaSemanaNovo` contém os novos dias do evento |


## 🪵 Logs Estruturados (JSON + MDC)


Em ambientes de produção, todos os logs são emitidos em **JSON** — uma linha por evento —, prontos para ingestão em ferramentas como **ELK Stack (Elasticsearch + Kibana)**, **Grafana Loki** ou **AWS CloudWatch Logs Insights**.

A rastreabilidade entre logs é garantida pelo **MDC (Mapped Diagnostic Context)** do SLF4J: no início do consumo de cada mensagem Kafka, a aplicação popula automaticamente um conjunto de campos contextuais que aparecem em **todas** as linhas de log geradas ao longo do mesmo processamento — sem nenhuma alteração nas assinaturas dos métodos internos.

---

### Campos MDC populados por processamento

| Campo | Quando é populado | Descrição |
|---|---|---|
| `correlationId` | Entrada do consumer (`TurmaAtualizadaConsumer`) | UUID único gerado por mensagem — correlaciona todos os logs de um mesmo evento de ponta a ponta |
| `businessKey` | Entrada do consumer | Chave de negócio da turma recebida no evento |
| `cicloId` | Entrada do consumer | ID do ciclo recebido no evento |
| `matriculaId` | Loop de processamento no `Service` | ID da matrícula sendo processada naquele momento |
| `alunoId` | Loop de processamento no `Service` | ID do aluno da matrícula em processamento |

> O MDC é **sempre limpo** ao final do processamento — inclusive em caso de exceção —, graças ao padrão `try-with-resources` implementado em `MdcContext`.

---

### Exemplo de saída JSON (produção)

```json
{
  "timestamp": "2026-06-16T10:23:45.100Z",
  "level": "INFO",
  "message": "Evento recebido do tópico turma-atualizada",
  "logger": "c.c.m.i.a.i.kafka.TurmaAtualizadaConsumer",
  "app": "matricula-process",
  "correlationId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "businessKey": "TURMA-2024-001",
  "cicloId": "42"
}
```

---


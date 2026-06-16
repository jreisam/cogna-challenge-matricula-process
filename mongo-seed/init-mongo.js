// Script de inicialização da coleção 'matriculas'
// Executado automaticamente pelo MongoDB na primeira inicialização do container

db = db.getSiblingDB('matriculas');

db.matriculas.drop();

db.matriculas.insertMany([
  // Documento 1 — dias DIFERENTES do evento → deve atualizar e publicar
  {
    alunoId: "ALU-001",
    businessKey: "GRAD/ENG/BACH/PRESENCIAL/NOTURNO/UNIT-SP-01",
    status: "ATIVA",
    turma: {
      codigo: "T2026-001",
      diasDaSemana: ["SEGUNDA", "QUARTA"],
      horarioInicio: "19:00",
      horarioFim: "22:30"
    },
    cicloId: NumberLong(20261),
    dataMatricula: ISODate("2026-02-10T08:00:00.000Z")
  },

  // Documento 2 — dias IGUAIS ao evento → não faz nada
  {
    alunoId: "ALU-002",
    businessKey: "GRAD/ENG/BACH/PRESENCIAL/NOTURNO/UNIT-SP-01",
    status: "ATIVA",
    turma: {
      codigo: "T2026-001",
      diasDaSemana: ["SEGUNDA", "QUARTA", "SEXTA"],
      horarioInicio: "19:00",
      horarioFim: "22:30"
    },
    cicloId: NumberLong(20261),
    dataMatricula: ISODate("2026-02-12T09:00:00.000Z")
  },

  // Documento 3 — status CANCELADA → deve ser ignorado (filtro por status ATIVA)
  {
    alunoId: "ALU-003",
    businessKey: "GRAD/ENG/BACH/PRESENCIAL/NOTURNO/UNIT-SP-01",
    status: "CANCELADA",
    turma: {
      codigo: "T2026-001",
      diasDaSemana: ["SEGUNDA", "QUARTA"],
      horarioInicio: "19:00",
      horarioFim: "22:30"
    },
    cicloId: NumberLong(20261),
    dataMatricula: ISODate("2026-02-10T10:00:00.000Z")
  },

  // Documento 4 — businessKey DIFERENTE do evento → não deve ser afetado
  {
    alunoId: "ALU-004",
    businessKey: "GRAD/ADM/BACH/EAD/NOTURNO/UNIT-RJ-02",
    status: "ATIVA",
    turma: {
      codigo: "T2026-050",
      diasDaSemana: ["TERÇA", "QUINTA"],
      horarioInicio: "19:00",
      horarioFim: "22:30"
    },
    cicloId: NumberLong(20261),
    dataMatricula: ISODate("2026-02-11T14:00:00.000Z")
  }
]);

print("✅ Seed concluído: " + db.matriculas.countDocuments() + " documentos inseridos na coleção 'matriculas'.");

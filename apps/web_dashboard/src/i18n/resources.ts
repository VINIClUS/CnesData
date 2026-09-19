export const resources = {
  meta: {
    title: "Recursos | CnesData",
    description:
      "Veja como o CnesData está sendo desenvolvido para organizar, conferir e apresentar dados do CNES.",
  },
  hero: {
    eyebrow: "Recursos",
    lead: "Conheça os recursos",
    highlight: "do CnesData",
    description:
      "Veja como o projeto está sendo desenvolvido para organizar, conferir e apresentar dados do CNES.",
  },
  blocks: [
    {
      id: "organizacao",
      eyebrow: "Organização dos dados",
      title: "Fontes, competência e contexto em um só lugar",
      description:
        "Os dados do CNES chegam de fontes diferentes e mudam a cada competência. O CnesData está sendo desenvolvido para manter cada registro associado à sua fonte, à competência e ao município consultado.",
      bullets: [
        "Cada registro identificado por fonte e competência",
        "Consulta sempre no contexto de um município",
        "Histórico preservado entre competências",
      ],
      stage: "desenvolvimento",
    },
    {
      id: "conferencia",
      eyebrow: "Conferência e consistência",
      title: "Divergências visíveis antes de virarem retrabalho",
      description:
        "Quando o mesmo profissional ou estabelecimento aparece com valores diferentes entre bases, a interface pretende destacar a divergência e apoiar a revisão, sem decidir por você.",
      bullets: [
        "Comparação campo a campo entre bases",
        "Situação de cada item marcada para revisão",
        "Registro do que já foi conferido",
      ],
      stage: "desenvolvimento",
    },
    {
      id: "indicadores",
      eyebrow: "Indicadores e acompanhamento",
      title: "Um panorama para acompanhar a competência",
      description:
        "O painel ilustrado na página inicial mostra a direção do projeto: totais, evolução e distribuição por tipo. Os gráficos são exemplos; nem todo indicador ilustrado já tem dados reais por trás.",
      bullets: [
        "Totais de estabelecimentos, profissionais e equipes",
        "Evolução ao longo das competências",
        "Distribuição por tipo de estabelecimento",
      ],
      stage: "desenvolvimento",
    },
  ],
  visuals: {
    kpis: "Prévia ilustrativa dos totais",
    divergence: {
      caption: "Exemplo ilustrativo de divergência",
      columns: ["Campo", "Base local", "Base nacional", "Situação"],
      rows: [
        ["CBO do profissional", "225125", "225142", "Revisar"],
        ["Carga horária", "40h", "20h", "Revisar"],
        ["Vínculo", "Ativo", "Ativo", "Conferido"],
      ],
    },
    charts: "Prévia ilustrativa dos indicadores",
  },
  flow: {
    eyebrow: "Como o fluxo se organiza",
    title: "Coleta, processamento e consulta",
    description:
      "Cada etapa é desenvolvida separadamente e identificada pelo seu estágio. Existência de código não significa disponibilidade: os estágios são atualizados conforme a verificação em ambiente real.",
    steps: [
      {
        title: "Coleta",
        description: "Extração dos dados do CNES na origem, por competência.",
        stage: "desenvolvimento",
      },
      {
        title: "Processamento",
        description: "Padronização e conferência dos registros recebidos.",
        stage: "desenvolvimento",
      },
      {
        title: "Consulta",
        description: "Interface para revisar e acompanhar os dados por competência.",
        stage: "desenvolvimento",
      },
    ],
  },
  cta: {
    title: "Participe do desenvolvimento",
    subtitle:
      "Conte como você utiliza os dados do CNES hoje. Quem participa dos primeiros testes ajuda a definir o que entra primeiro.",
  },
} as const;

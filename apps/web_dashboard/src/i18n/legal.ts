export type LegalSection = { title: string; paragraphs: readonly string[]; pending?: boolean };

export type LegalDoc = {
  slug: "privacidade" | "termos" | "ajuda";
  meta: { title: string; description: string };
  eyebrow: string;
  lead: string;
  highlight: string;
  description: string;
  updatedAt: string;
  sections: readonly LegalSection[];
};

const _PENDING = "Ponto a definir pelo responsável do projeto antes do lançamento público.";

export const PRIVACY_EMAIL = "privacidade@vinisantana.com";

/** Retenção definida para os registros do formulário público. */
export const LEADS_RETENTION_MONTHS = 12;

/**
 * Nome ou razão social do controlador. Vazio enquanto a entidade definitiva não
 * for informada; nesse caso a seção correspondente é marcada como pendente.
 */
const _CONTROLLER_NAME: string = "";

function _controllerParagraph(): string {
  if (_CONTROLLER_NAME) {
    return `Controlador dos dados: ${_CONTROLLER_NAME}. Pedidos sobre seus dados: ${PRIVACY_EMAIL}.`;
  }
  return (
    "Controlador dos dados: o nome ou razão social definitivo ainda não foi informado e será " +
    `publicado nesta página. Pedidos sobre seus dados: ${PRIVACY_EMAIL}.`
  );
}

export const legal = {
  pendingLabel: "A definir",
  pendingNote: _PENDING,
  updatedLabel: "Última atualização",
  backToContact: "Falar com o projeto",
  privacidade: {
    slug: "privacidade",
    meta: {
      title: "Política de privacidade | CnesData",
      description: "Quais dados o formulário de contato do CnesData coleta e como são usados.",
    },
    eyebrow: "Privacidade",
    lead: "Política de",
    highlight: "privacidade",
    description:
      "Esta política descreve apenas o que o site público do CnesData coleta hoje. Ela será revisada quando o produto entrar em uso real.",
    updatedAt: "18 de setembro de 2026",
    sections: [
      {
        title: "Quem é o responsável",
        paragraphs: [
          "O CnesData é um projeto independente, em desenvolvimento, mantido pela pessoa responsável indicada na página Sobre. Não há vínculo com Ministério da Saúde, DATASUS ou prefeituras.",
          _controllerParagraph(),
        ],
        pending: !_CONTROLLER_NAME,
      },
      {
        title: "Quais dados o formulário de contato coleta",
        paragraphs: [
          "Obrigatórios: nome e e-mail. Opcionais, apenas se você preencher: instituição ou município, função e a mensagem livre.",
          "Também registramos o tipo de interesse selecionado (acesso antecipado, piloto ou contato geral), a página de origem dentro do site, a versão desta política e a data e hora do envio.",
          "Não coletamos CPF, CNPJ, telefone, dados de pacientes, anexos ou credenciais de sistemas. Pedimos que você não inclua esses dados no campo de mensagem.",
          "Para conter envios automatizados, o servidor usa o endereço IP apenas em memória, para limitar a quantidade de envios por conexão. O IP não é gravado no banco de dados junto com o seu contato.",
        ],
      },
      {
        title: "Para que usamos",
        paragraphs: [
          "Somente para responder ao seu contato e, se você indicou interesse, para conversar sobre acesso antecipado ou piloto. Não enviamos campanhas, não há lista de e-mail marketing e não compartilhamos os dados com terceiros.",
        ],
      },
      {
        title: "Onde os dados ficam",
        paragraphs: [
          "Os dados do formulário são gravados no banco de dados do próprio CnesData, hospedado em servidor sob controle do projeto. O acesso é restrito à pessoa responsável pelo projeto.",
        ],
      },
      {
        title: "Por quanto tempo guardamos",
        paragraphs: [
          `O prazo definido é de ${LEADS_RETENTION_MONTHS} meses após a última interação sobre o seu contato.`,
          "O que o sistema registra é a data do envio do formulário. Conversas posteriores acontecem por e-mail, fora do banco de dados, por isso a contagem parte dessa data e é ajustada manualmente quando houver troca de mensagens depois.",
          "A exclusão é feita manualmente pela pessoa responsável: não existe hoje rotina automática de expurgo no sistema.",
        ],
      },
      {
        title: "Seus direitos",
        paragraphs: [
          `Você pode pedir a qualquer momento para ver, corrigir ou apagar os dados que enviou. O canal de privacidade é ${PRIVACY_EMAIL}.`,
          "Escreva a partir do mesmo endereço usado no formulário: é por ele que localizamos o seu registro, já que não guardamos nenhum outro identificador.",
          "Os pedidos são atendidos manualmente pela pessoa responsável pelo projeto. Nesta fase não há encarregado designado nem prazo de resposta definido.",
        ],
      },
      {
        title: "Cookies e rastreamento",
        paragraphs: [
          "O site público não usa cookies de rastreamento nem ferramentas de analytics. Guardamos apenas a sua preferência de tema (claro ou escuro) no navegador; ela não sai do seu dispositivo.",
        ],
      },
    ],
  },
  termos: {
    slug: "termos",
    meta: {
      title: "Termos de uso | CnesData",
      description: "Condições de uso do site público do CnesData durante o desenvolvimento.",
    },
    eyebrow: "Termos",
    lead: "Termos de uso",
    highlight: "do site",
    description:
      "Estes termos valem para o site público durante a fase de desenvolvimento. Termos do produto e de eventuais planos serão publicados separadamente.",
    updatedAt: "18 de setembro de 2026",
    sections: [
      {
        title: "O que este site é",
        paragraphs: [
          "O site apresenta um projeto em desenvolvimento. As telas mostradas são prévias ilustrativas: não representam dados reais de nenhum município nem resultados de processamento em produção.",
        ],
      },
      {
        title: "Acesso antecipado e piloto",
        paragraphs: [
          "Enviar interesse pelo formulário não cria conta, não aprova acesso e não garante participação em piloto. Qualquer participação é combinada individualmente, por e-mail, antes de qualquer acesso a dados.",
        ],
      },
      {
        title: "Uso aceitável",
        paragraphs: [
          "Não é permitido usar o formulário para envio automatizado, spam ou conteúdo que não se refira ao CnesData. Envios abusivos podem ser bloqueados por limite de tentativas.",
        ],
      },
      {
        title: "Sem garantias nesta fase",
        paragraphs: [
          "O site e as funcionalidades descritas podem mudar ou ser descontinuados sem aviso enquanto o projeto estiver em desenvolvimento. Nada aqui constitui oferta comercial.",
        ],
      },
      {
        title: "Planos, preços e contratação",
        paragraphs: [
          "Condições comerciais, contratação e termos do produto não estão definidos nesta fase e não fazem parte destes termos.",
        ],
        pending: true,
      },
      {
        title: "Foro e legislação",
        paragraphs: ["Legislação aplicável e foro ainda não foram definidos."],
        pending: true,
      },
    ],
  },
  ajuda: {
    slug: "ajuda",
    meta: {
      title: "Ajuda | CnesData",
      description: "Como falar com o projeto CnesData e o que esperar de cada canal.",
    },
    eyebrow: "Ajuda",
    lead: "Precisa de",
    highlight: "ajuda?",
    description:
      "O CnesData ainda não tem central de atendimento. Estes são os canais disponíveis durante o desenvolvimento.",
    updatedAt: "18 de setembro de 2026",
    sections: [
      {
        title: "Formulário de contato",
        paragraphs: [
          "Use a página Contato para interesse em acesso antecipado, piloto ou perguntas gerais. Respondemos pelo e-mail informado, sem prazo garantido nesta fase.",
        ],
      },
      {
        title: "E-mail direto",
        paragraphs: [
          "Se o formulário estiver indisponível ou você preferir e-mail, escreva para o endereço abaixo. Não envie dados de pacientes, senhas ou documentos.",
        ],
      },
      {
        title: "Pedidos sobre seus dados",
        paragraphs: [
          `Para ver, corrigir ou apagar o que você enviou pelo formulário, escreva para ${PRIVACY_EMAIL} usando o mesmo endereço do envio. Os detalhes estão na política de privacidade.`,
        ],
      },
      {
        title: "Problemas para entrar",
        paragraphs: [
          "O acesso ao painel é restrito a quem já participa dos testes. Se você recebeu credenciais e não consegue entrar, informe pelo e-mail o município e a mensagem de erro exibida.",
        ],
      },
      {
        title: "Dúvidas sobre o CNES em si",
        paragraphs: [
          "O CnesData não é um canal do Ministério da Saúde nem do DATASUS. Dúvidas sobre cadastro, prazos ou regras do CNES devem ser tratadas nos canais oficiais.",
        ],
      },
    ],
  },
} as const;

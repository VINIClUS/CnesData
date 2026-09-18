export const contact = {
  meta: {
    title: "Contato | CnesData",
    description:
      "Converse sobre o CnesData: acesso antecipado, participação em piloto ou contato geral.",
  },
  eyebrow: "Contato",
  hero: {
    contato: {
      lead: "Converse sobre",
      highlight: "o CnesData",
      description:
        "Dúvidas, sugestões ou interesse em acompanhar o projeto. Responderemos pelo e-mail informado.",
    },
    "acesso-antecipado": {
      lead: "Participe do",
      highlight: "acesso antecipado",
      description:
        "Conte como você utiliza os dados do CNES hoje. As primeiras pessoas a testar ajudam a definir o que entra primeiro.",
    },
    piloto: {
      lead: "Tenho interesse em participar",
      highlight: "de um piloto",
      description:
        "Um piloto envolve conferir dados reais do seu município com acompanhamento próximo durante o desenvolvimento.",
    },
  },
  intro: {
    eyebrow: "Como funciona",
    title: "O que esperar desta conversa",
    description:
      "O CnesData está em desenvolvimento. Este formulário registra seu interesse; não cria conta, não aprova acesso e não é um canal de atendimento do CNES.",
    bullets: [
      "Lemos cada mensagem e respondemos pelo e-mail informado.",
      "Interesse em piloto passa por uma conversa antes de qualquer acesso.",
      "Você pode ajustar o tipo de interesse no formulário.",
    ],
    sensitive:
      "Não envie dados de pacientes, senhas, CPF, CNPJ, anexos ou credenciais de sistemas.",
  },
  form: {
    title: "Conte seu interesse",
    labels: {
      name: "Nome",
      email: "E-mail",
      interest: "Interesse",
      organization: "Instituição ou município",
      role: "Função",
      message: "O que você gostaria de resolver com o CnesData?",
    },
    optional: "(opcional)",
    interests: {
      early_access: "Acesso antecipado",
      pilot: "Participar de um piloto",
      contact: "Contato geral",
    },
    messageHint: "Não inclua dados de pacientes, senhas ou documentos.",
    privacy:
      "Usamos nome e e-mail apenas para responder sobre o CnesData. Não compartilhamos seus dados nem enviamos campanhas.",
    submit: "Enviar interesse",
    submitting: "Enviando...",
  },
  errors: {
    nameRequired: "Informe seu nome.",
    nameMax: "O nome deve ter até 120 caracteres.",
    emailRequired: "Informe seu e-mail.",
    emailInvalid: "Informe um e-mail válido.",
    emailMax: "O e-mail deve ter até 254 caracteres.",
    organizationMax: "A instituição deve ter até 200 caracteres.",
    roleMax: "A função deve ter até 120 caracteres.",
    messageMax: "A mensagem deve ter até 2000 caracteres.",
  },
  status: {
    received:
      "Recebemos seu interesse no CnesData. Isso não cria uma conta nem garante participação no piloto.",
    failed: {
      validation: "Alguns campos não foram aceitos. Revise as informações e tente novamente.",
      rate_limited: "Muitas tentativas em pouco tempo. Aguarde alguns minutos e tente novamente.",
      unavailable:
        "Não foi possível registrar seu interesse agora: o serviço está indisponível. Tente novamente ou envie um e-mail.",
      network:
        "Não foi possível registrar seu interesse agora: falha de conexão. Tente novamente ou envie um e-mail.",
    },
    retry: "Tentar novamente",
    sendEmail: "Enviar e-mail",
  },
  mailSubject: {
    early_access: "Acesso antecipado ao CnesData",
    pilot: "Piloto do CnesData",
    contact: "Contato sobre o CnesData",
  },
} as const;

import { marketing } from "@/i18n/marketing";

export type Plan = {
  id: "basico" | "profissional" | "enterprise";
  name: string;
  description: string;
  badge: string;
  originalPrice: string;
  price: string;
  features: readonly string[];
  cta: { label: string; href: string };
};

const _SALES = `mailto:${marketing.contactEmail}?subject=Plano%20Enterprise`;

export const pricing = {
  hero: {
    eyebrow: "Planos e preços",
    lead: "Planos simples para",
    highlight: "municípios e consórcios",
    description:
      "Escolha o plano ideal e tenha acesso a dados do CNES, dashboards estratégicos e ferramentas para uma gestão pública mais eficiente.",
    bullets: ["Sem fidelidade", "Ativação imediata", "Suporte especializado"],
    card: {
      title: "Mesma confiança. Mais resultados para a sua gestão.",
      description:
        "CnesData é a solução completa para transformar dados do CNES em informação estratégica para o seu município.",
    },
  },
  priceLabels: { from: "De", perOnly: "por apenas", installments: "12x de" },
  plans: [
    {
      id: "basico",
      name: "Básico",
      description: "Ideal para municípios de pequeno porte",
      badge: "Economize 50%",
      originalPrice: "R$ 2.988",
      price: "R$ 149",
      features: [
        "Até 10 usuários",
        "Dashboards essenciais",
        "Relatórios padrão",
        "Suporte por e-mail",
        "Atualizações automáticas",
        "Acesso a dados históricos do CNES",
      ],
      cta: { label: "Começar agora", href: "/login" },
    },
    {
      id: "profissional",
      name: "Profissional",
      description: "Para municípios em crescimento",
      badge: "Mais popular",
      originalPrice: "R$ 5.988",
      price: "R$ 299",
      features: [
        "Até 50 usuários",
        "Todos os recursos do plano Básico",
        "Relatórios avançados e personalizáveis",
        "Dashboards com indicadores e tendências",
        "Suporte prioritário",
        "Exportação de dados (Excel, PDF)",
        "Ferramentas para planejamento e tomada de decisão",
      ],
      cta: { label: "Começar agora", href: "/login" },
    },
    {
      id: "enterprise",
      name: "Enterprise",
      description: "Para grandes municípios e consórcios",
      badge: "Economize 44%",
      originalPrice: "R$ 14.388",
      price: "R$ 799",
      features: [
        "Usuários ilimitados",
        "Todos os recursos do plano Profissional",
        "Customizações e integrações",
        "Relatórios sob demanda",
        "Suporte dedicado (SLA)",
        "Consultoria para implementação",
        "Recursos exclusivos para consórcios",
      ],
      cta: { label: "Falar com vendas", href: _SALES },
    },
  ] as const satisfies readonly Plan[],
  trust: [
    { title: "Pagamento seguro", description: "Seus dados protegidos e transações 100% seguras." },
    { title: "Nota fiscal", description: "Emissão de nota fiscal para órgãos públicos." },
    {
      title: "Ativação imediata",
      description: "Comece a usar agora mesmo após a confirmação do pagamento.",
    },
    {
      title: "Suporte especializado",
      description: "Nossa equipe está pronta para ajudar você em todas as etapas.",
    },
  ],
  faq: {
    eyebrow: "Dúvidas frequentes",
    title: "Perguntas mais comuns",
    description: "Tire suas dúvidas sobre planos, pagamento e funcionalidades do CnesData.",
    button: "Ver todas as perguntas",
    items: [
      {
        question: "Posso alterar meu plano depois?",
        answer: "Sim. Você pode fazer upgrade ou downgrade a qualquer momento pelo painel.",
      },
      {
        question: "O plano inclui atualização dos dados do CNES?",
        answer: "Sim. Todos os planos recebem atualizações automáticas a cada competência.",
      },
      {
        question: "Existe fidelidade?",
        answer: "Não. Você pode cancelar quando quiser, sem multa.",
      },
      {
        question: "É possível solicitar uma demonstração?",
        answer: "Sim. Entre em contato e agendamos uma demonstração guiada com a sua equipe.",
      },
      {
        question: "Quais formas de pagamento são aceitas?",
        answer: "Cartão de crédito, boleto bancário e empenho para órgãos públicos.",
      },
      {
        question: "Vocês oferecem plano para consórcios?",
        answer: "Sim. O plano Enterprise inclui recursos exclusivos para consórcios de saúde.",
      },
    ],
  },
  cta: "Pronto para transformar dados em resultados?",
} as const;

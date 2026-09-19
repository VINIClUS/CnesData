import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { LegalPage } from "@/components/legal/LegalPage";
import type { LegalSection } from "@/i18n/legal";
import { PRIVACY_EMAIL, legal } from "@/i18n/legal";

describe("LegalPage", () => {
  test.each([
    ["privacidade", "Política de privacidade"],
    ["termos", "Termos de uso do site"],
    ["ajuda", "Precisa de ajuda?"],
  ] as const)("renderiza_%s_com_h1_e_secoes", async (slug, h1) => {
    const { container } = renderWithRouter(<LegalPage doc={legal[slug]} />, `/${slug}`);
    expect(await screen.findByRole("heading", { level: 1, name: h1 })).toBeInTheDocument();
    expect(container.querySelectorAll("article h2")).toHaveLength(legal[slug].sections.length);
    expect(screen.getByText(/Última atualização/)).toBeInTheDocument();
  });

  test("privacidade_lista_dados_coletados_e_uso_de_ip_em_memoria", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByText(/Obrigatórios: nome e e-mail/)).toBeInTheDocument();
    expect(screen.getByText(/Não coletamos CPF, CNPJ/)).toBeInTheDocument();
    expect(screen.getByText(/endereço IP apenas em memória/)).toBeInTheDocument();
  });

  test("privacidade_declara_retencao_de_12_meses_e_exclusao_manual", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByText(/12 meses após a última interação/)).toBeInTheDocument();
    expect(screen.getByText(/não existe hoje rotina automática de expurgo/)).toBeInTheDocument();
    expect(screen.getByText(/a contagem parte dessa data/)).toBeInTheDocument();
  });

  test("privacidade_identifica_o_controlador", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(
      screen.getByText(/O responsável pelo tratamento é Vinicius A\. de Santana/),
    ).toBeInTheDocument();
  });

  test("privacidade_declara_canal_formal_sem_prometer_prazo_ou_encarregado", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getAllByText(new RegExp(PRIVACY_EMAIL)).length).toBeGreaterThanOrEqual(2);
    expect(
      screen.getByText(/Canal formal de atendimento para assuntos de privacidade/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/prazo de resposta/)).toBeNull();
    expect(screen.queryByText(/encarregado/)).toBeNull();
  });

  test("privacidade_nao_tem_secao_pendente", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.queryByText("A definir")).toBeNull();
    const sections: readonly LegalSection[] = legal.privacidade.sections;
    expect(sections.filter((s) => s.pending)).toHaveLength(0);
  });

  test("oferece_email_e_link_para_contato", async () => {
    renderWithRouter(<LegalPage doc={legal.ajuda} />, "/ajuda");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByRole("link", { name: /me@vinisantana\.com/ })).toHaveAttribute(
      "href",
      "mailto:me@vinisantana.com",
    );
    expect(screen.getByRole("link", { name: /Falar com o projeto/ })).toHaveAttribute(
      "href",
      "/contato",
    );
  });
});

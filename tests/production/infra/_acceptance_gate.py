"""Testes exigidos por task no gate de aceite PROD-INFRA-012."""

GATE: dict[str, dict[str, tuple[str, ...]]] = {
    "PROD-INFRA-001": {"test_composition.py": (
        "test_configuracao_opentofu_e_valida", "test_providers_definem_regioes_e_tags_nativas",
        "test_locals_de_nome_e_tags_sao_avaliados_pelo_opentofu",
        "test_grafo_do_produto_nao_contem_recurso_compartilhado",
        "test_validacao_nativa_rejeita_shared_outputs_invalido",
        "test_inventario_de_outputs_publica_interfaces_do_static_web")},
    "PROD-INFRA-002": {"test_control_plane.py": (
        "test_raiz_agenda_tabela_do_control_plane",
        "test_tabela_tem_inventario_exato_de_chaves_e_indices", "test_tabela_tem_prevent_destroy")},
    "PROD-INFRA-003": {
        "test_data_buckets.py": (
            "test_raiz_agenda_buckets_de_dados_e_auditoria",
            "test_tls_obrigatorio_e_owner_enforced",
            "test_audit_tem_object_lock_compliance_365_na_criacao",
            "test_audit_sem_politica_de_bypass_ou_delete"),
        "test_data_buckets_protection.py": ("test_bucket_tem_prevent_destroy",)},
    "PROD-INFRA-004": {"test_cognito.py": (
        "test_raiz_agenda_cognito_com_sufixo_revisado", "test_client_publico_pkce_sem_secret",
        "test_pool_tem_deletion_protection_e_prevent_destroy",
        "test_outputs_batem_com_settings_de_producao")},
    "PROD-INFRA-005": {
        "test_static_web.py": (
            "test_origem_privada_separa_allow_oac_e_deny_tls",
            "test_distribuicao_liga_s3_oac_waf_acm_e_funcao",
            "test_headers_cache_e_lifecycle_respeitam_ruling",),
        "test_static_web_composition.py": (
            "test_raiz_agenda_static_web_e_acm_do_produto",
            "test_outputs_cloudfront_e_runtime_sao_utilizaveis")},
    "PROD-INFRA-006": {
        "test_network.py": (
            "test_rede_sem_nat_lb_ou_listener", "test_security_group_sem_ingress"),
        "test_network_guards.py": (
            "test_grupos_declaram_listas_vazias_para_remover_regra_externa",)},
    "PROD-INFRA-007": {
        "test_ecr_ecs.py": (
            "test_ecr_scan_on_push_e_lifecycle_limitado",
            "test_execution_role_so_pull_e_logs_proprios",
            "test_root_anexa_inventario_exato_de_policies_as_roles"),
        "test_processing_ecs.py": (
            "test_outro_digest_altera_imagem_dos_tres_modos", "test_image_uri_exige_digest"),
        "test_processing_security.py": (
            "test_container_exige_raiz_somente_leitura_usuario_app_e_sem_privilegios",
            "test_dockerfile_declara_volume_dos_mounts_depois_do_chown"),
        "test_fargate_task_mirror.py": (
            "test_espelho_container_e_task_iguais_ao_modo_recovery_renderizado",
            "test_espelho_com_entrypoint_do_probe_difere_so_no_entrypoint")},
    "PROD-INFRA-008": {
        "test_step_functions_iam.py": (
            "test_role_step_functions_tem_trust_e_grants_common_exatos_no_bootstrap",),
        "test_step_functions_iam_asl.py": (
            "test_definition_production_tem_map_inline_e_retry_exato",),
        "test_processor_revision_contract.py": (
            "test_run_task_sem_digest_espelha_retained_exato_e_ignora_ordem",)},
    "PROD-INFRA-009": {
        "test_schedulers.py": (
            "test_schedules_a_cada_30_min_sem_retry",
            "test_scheduler_trust_exato_e_pass_role_restrito",
            "test_orcamento_de_96_task_horas_em_30_dias"),
        "test_observability_alarms.py": (
            "test_alarme_de_horas_de_task_e_proxy_semanal_de_unit_tasks",
            "test_regras_de_evento_da_task_unit_filtram_o_cluster_e_a_familia")},
    "PROD-INFRA-010": {"test_canary_fence_iam.py": (
        "test_cada_role_recebe_a_matriz_exata_da_release", "test_operador_muta_so_o_fence",
        "test_verifier_nega_outro_release_id", "test_confianca_exata_das_roles",
        "test_roles_novas_sem_curinga_scan_delete_list_ou_bypass",
        "test_padrao_desligado_nao_cria_recursos_de_canary")},
    "PROD-INFRA-011": {
        "test_athena_cutoff_iam.py": (
            "test_matriz_negativa_da_role_de_cutoff",
            "test_contrato_de_cutoff_exposto_na_raiz_tem_os_valores_fixados",
            "test_unico_par_role_acao_athena_e_o_update_work_group_do_cutoff",
            "test_unico_ignore_changes_e_o_estado_do_workgroup"),
        "test_athena_observability_cost.py": (
            "test_athena_fora_das_roles_do_api", "test_freeze_preserva_audit_e_backup",
            "test_manifest_do_produto_ate_8_e_agregado_ate_15"),
        "test_cost_manifest.py": (
            "test_limites_fixos_do_produto_cognito_e_waf", "test_fronteira_dos_limites_8_e_15"),
        "test_opa_policy.py": ("test_fixture_rejeitada_falha_com_mensagem_exata",)},
    "PROD-INFRA-441": {"test_s3_missing_object_invariant.py": (
        "test_get_object_de_cada_role_fica_coberto_pelo_list_bucket_condicionado",)},
}

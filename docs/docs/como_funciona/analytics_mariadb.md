## Persistência de analytics (MariaDB)

O Ro-dou gera relatórios de busca a partir de YAML e executa as buscas em tasks `exec_search_*`.
O “relatório” é o payload retornado por essas tasks via XCom (um dicionário aninhado com grupos → termos → departamentos → itens).

Esta funcionalidade adiciona uma task **obrigatória** (`persist_analytics`) logo após `exec_searchs` para:

- persistir **achados granulares** (um registro por ocorrência encontrada);
- persistir um **resumo por termo/dia** (contagem de ocorrências por termo).

### Por que esta abordagem é viável

- **Segue o padrão do projeto**: reaproveita o mesmo XCom usado para e-mail/Slack/Discord, sem depender de arquivos extras.
- **Granularidade máxima**: permite medir efetividade por usuário/owner, termo, seção, tipo de publicação, etc.
- **Idempotente**: usa `finding_hash` como chave primária, então reprocessamentos não duplicam linhas.
- **MariaDB nativo via Airflow**: usa `MySqlHook` (compatível com MariaDB) e `INSERT ... ON DUPLICATE KEY`.

### Configuração

Não há configuração via YAML para analytics.

- **Airflow Connection**: `rodou_analytics_db`
- **Tabelas**: `rodou_search_findings` e `rodou_search_term_summary`

### O que vai para o banco

- `rodou_search_findings`: 1 linha por achado
  - `dag_id`, `owner`, `trigger_date`, `term`, `department`, `section`, `pubtype`, `title`, `href`, etc.
- `rodou_search_term_summary`: 1 linha por (termo, departamento, dia)
  - `match_count` atualizado via upsert

### Dependência no Airflow

Na imagem/ambiente do Airflow, garanta o provider do MySQL:

- `apache-airflow-providers-mysql`

### Observações

- A task `persist_analytics` roda mesmo quando `skip_null` está ativo (assim você consegue medir dias “zero-match” no resumo, se desejar expandir para registrar zeros).
- Para registrar explicitamente “zero match” por termo, dá para estender a lógica para enumerar todos os termos configurados no YAML e gravar `match_count=0` quando ausente.

- Analytics é persistido **somente** para buscas que incluem a fonte `INLABS`.


from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from airflow.hooks.base import BaseHook


class AnalyticsMariaDBHook(BaseHook):
    """
    Minimal hook to persist search analytics to MariaDB.

    Uses Airflow's MySQL provider hook (compatible with MariaDB).
    """

    def __init__(self, conn_id: str, database: Optional[str] = None):
        super().__init__()
        self.conn_id = conn_id
        self.database = database

    def _mysql_hook(self):
        try:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
        except Exception as e:  # pragma: no cover
            raise ImportError(
                "Missing dependency `apache-airflow-providers-mysql`. "
                "Install it in the Airflow environment to enable MariaDB analytics."
            ) from e

        return MySqlHook(mysql_conn_id=self.conn_id, schema=self.database)

    @dataclass(frozen=True)
    class ExecutionReference:
        id_preferencia: str
        id_dag_gerada: int

    def resolve_execution_reference(self, *, dag_id: str, owner: str) -> "AnalyticsMariaDBHook.ExecutionReference":
        """Resolve the generated DAG row for the current execution.

        `dag_id` may come with "pref_" prefix (e.g., "pref_12345").
        Extracts the raw `id_preferencia` for the database query.
        """
        # Extract id_preferencia from dag_id: "pref_12345" -> "12345"
        id_preferencia_raw = dag_id.replace("pref_", "", 1) if dag_id.startswith("pref_") else dag_id
        
        hook = self._mysql_hook()
        query = """
            SELECT id_dag_gerada, id_preferencia
            FROM tb_clipping_dag_gerada
            WHERE nm_dag = %s
              AND id_preferencia = %s
            ORDER BY dh_geracao DESC, id_dag_gerada DESC
            LIMIT 1
        """
        # nm_dag is the full dag_id (possibly with "pref_" prefix as stored in the DB)
        # id_preferencia is the extracted numeric/string ID without prefix
        rows = hook.get_records(query, parameters=(dag_id, id_preferencia_raw))
        if not rows:
            raise RuntimeError(
                f"Could not resolve TB_CLIPPING_DAG_GERADA for dag_id={dag_id!r}, id_preferencia={id_preferencia_raw!r}"
            )
        id_dag_gerada, id_preferencia = rows[0]
        return AnalyticsMariaDBHook.ExecutionReference(
            id_preferencia=str(id_preferencia),
            id_dag_gerada=int(id_dag_gerada),
        )

    def get_previous_result_hashes(
        self,
        *,
        id_preferencia: str,
        dag_id: str,
    ) -> set[str]:
        """Return the stable result hashes from the latest successful execution."""
        hook = self._mysql_hook()
        query = """
            SELECT r.cd_hash_resultado
            FROM tb_clipping_execucao_resultado r
            INNER JOIN tb_clipping_execucao e
                ON e.id_execucao = r.id_execucao
            WHERE e.id_execucao = (
                SELECT e2.id_execucao
                FROM tb_clipping_execucao e2
                WHERE e2.id_preferencia = %s
                  AND e2.cd_dag_id_airflow = %s
                  AND e2.id_status_execucao = 9
                ORDER BY e2.id_execucao DESC
                LIMIT 1
            )
        """
        rows = hook.get_records(query, parameters=(id_preferencia, dag_id))
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def insert_execution(
        self,
        *,
        id_dag_gerada: int,
        id_preferencia: str,
        cd_chave_usuario: str,
        id_status_execucao: int,
        id_origem_evento: int,
        cd_run_id_airflow: str,
        cd_dag_id_airflow: str,
        cd_state_airflow: Optional[str],
        dh_disparo,
        dh_inicio,
        dh_fim,
        qt_resultados_total: int,
        qt_resultados_enviados: int,
        qt_resultados_novos: int,
        qt_resultados_repetidos: int,
        fl_possui_resultado: int,
        fl_email_enviado: int,
        fl_csv_anexado: int,
        tx_retorno_execucao: Optional[str] = None,
        tx_erro_resumido: Optional[str] = None,
    ) -> int:
        hook = self._mysql_hook()
        conn = hook.get_conn()
        query = """
            INSERT INTO tb_clipping_execucao (
                id_dag_gerada,
                id_preferencia,
                cd_chave_usuario,
                id_status_execucao,
                id_origen_evento,
                cd_run_id_airflow,
                cd_dag_id_airflow,
                cd_state_airflow,
                tx_retorno_execucao,
                tx_erro_resumido,
                dh_disparo,
                dh_inicio,
                dh_fim,
                dt_criacao,
                qt_resultados_total,
                qt_resultados_enviados,
                qt_resultados_novos,
                qt_resultados_repetidos,
                fl_possui_resultado,
                fl_email_enviado,
                fl_csv_anexado
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s
            )
        """
        params = (
            id_dag_gerada,
            id_preferencia,
            cd_chave_usuario,
            id_status_execucao,
            id_origem_evento,
            cd_run_id_airflow,
            cd_dag_id_airflow,
            cd_state_airflow,
            tx_retorno_execucao,
            tx_erro_resumido,
            dh_disparo,
            dh_inicio,
            dh_fim,
            qt_resultados_total,
            qt_resultados_enviados,
            qt_resultados_novos,
            qt_resultados_repetidos,
            fl_possui_resultado,
            fl_email_enviado,
            fl_csv_anexado,
        )
        with conn.cursor() as cur:
            cur.execute(query, params)
            cur.execute("SELECT LAST_INSERT_ID()")
            row = cur.fetchone()
        conn.commit()
        return int(row[0])

    def update_execution_delivery(
        self,
        *,
        id_execucao: int,
        qt_resultados_enviados: int,
        fl_email_enviado: int,
        fl_csv_anexado: int,
        id_status_execucao: Optional[int] = None,
        cd_state_airflow: Optional[str] = None,
        tx_erro_resumido: Optional[str] = None,
        dh_fim=None,
    ) -> None:
        hook = self._mysql_hook()
        conn = hook.get_conn()
        fields = ["qt_resultados_enviados = %s", "fl_email_enviado = %s", "fl_csv_anexado = %s"]
        params: List[object] = [qt_resultados_enviados, fl_email_enviado, fl_csv_anexado]
        if id_status_execucao is not None:
            fields.append("id_status_execucao = %s")
            params.append(id_status_execucao)
        if cd_state_airflow is not None:
            fields.append("cd_state_airflow = %s")
            params.append(cd_state_airflow)
        if tx_erro_resumido is not None:
            fields.append("tx_erro_resumido = %s")
            params.append(tx_erro_resumido)
        if dh_fim is not None:
            fields.append("dh_fim = %s")
            params.append(dh_fim)

        sql = f"UPDATE tb_clipping_execucao SET {', '.join(fields)} WHERE id_execucao = %s"
        params.append(id_execucao)
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
        conn.commit()

    def mark_execution_results_sent(self, *, id_execucao: int) -> int:
        hook = self._mysql_hook()
        conn = hook.get_conn()
        sql = """
            UPDATE tb_clipping_execucao_resultado
            SET fl_enviado = 1
            WHERE id_execucao = %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (id_execucao,))
            affected = cur.rowcount
        conn.commit()
        return int(affected)

    def insert_execution_results(self, *, id_execucao: int, rows: Sequence[dict]) -> List[dict]:
        """Insert unique result rows for the execution and return the unique set."""
        if not rows:
            return []

        hook = self._mysql_hook()
        conn = hook.get_conn()
        unique_rows = []
        seen = set()
        for row in rows:
            hash_value = row["cd_hash_resultado"]
            if hash_value in seen:
                continue
            seen.add(hash_value)
            unique_rows.append(row)

        sql = """
            INSERT INTO tb_clipping_execucao_resultado (
                id_execucao,
                cd_hash_resultado,
                dt_publicacao,
                dt_criacao,
                ds_titulo_resultado,
                ds_url_resultado,
                ds_id_publicacao,
                fl_enviado
            ) VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
        """
        values = [
            (
                id_execucao,
                row.get("cd_hash_resultado"),
                row.get("dt_publicacao"),
                row.get("ds_titulo_resultado"),
                row.get("ds_url_resultado"),
                row.get("ds_id_publicacao"),
                int(bool(row.get("fl_enviado", 0))),
            )
            for row in unique_rows
        ]
        with conn.cursor() as cur:
            cur.executemany(sql, values)
        conn.commit()
        return unique_rows

    def ensure_schema(self, *, findings_table: str, summary_table: str) -> None:
        hook = self._mysql_hook()

        # Keep types generic and MariaDB-friendly. Use `finding_hash` as stable id.
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{findings_table}` (
          `finding_hash` CHAR(40) NOT NULL,
          `dag_id` VARCHAR(250) NOT NULL,
          `owner` VARCHAR(250) NOT NULL,
          `run_id` VARCHAR(250) NOT NULL,
          `logical_date` DATETIME NOT NULL,
          `trigger_date` DATE NOT NULL,
          `source` VARCHAR(50) NULL,
          `search_header` TEXT NULL,
          `group_name` VARCHAR(250) NOT NULL,
          `term` TEXT NOT NULL,
          `department` VARCHAR(250) NOT NULL,
          `section` VARCHAR(250) NULL,
          `pubtype` VARCHAR(250) NULL,
          `title` TEXT NULL,
          `href` TEXT NULL,
          `publication_date` VARCHAR(50) NULL,
          `publication_id` VARCHAR(64) NULL,
          `hierarchy` TEXT NULL,
          `ai_generated` BOOLEAN NULL,
          `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`finding_hash`),
          KEY `idx_trigger_date` (`trigger_date`),
          KEY `idx_dag_owner_date` (`dag_id`, `owner`, `trigger_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE IF NOT EXISTS `{summary_table}` (
          `dag_id` VARCHAR(250) NOT NULL,
          `owner` VARCHAR(250) NOT NULL,
          `trigger_date` DATE NOT NULL,
          `group_name` VARCHAR(250) NOT NULL,
          `term` TEXT NOT NULL,
          `department` VARCHAR(250) NOT NULL,
          `match_count` INT NOT NULL,
          `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `uniq_term_day` (`dag_id`, `owner`, `trigger_date`, `group_name`, `department`, `term`(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        logging.info(
            "Ensuring MariaDB analytics tables exist: %s, %s",
            findings_table,
            summary_table,
        )
        hook.run(sql, autocommit=True)

    def upsert_findings(self, *, table: str, rows: List[dict]) -> int:
        if not rows:
            return 0
        hook = self._mysql_hook()
        conn = hook.get_conn()
        cols = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_sql = ", ".join([f"`{c}`" for c in cols])

        # Only insert; hash is PK so duplicates are ignored safely.
        sql = f"INSERT IGNORE INTO `{table}` ({col_sql}) VALUES ({placeholders})"

        values = [tuple(r.get(c) for c in cols) for r in rows]
        with conn.cursor() as cur:
            cur.executemany(sql, values)
        conn.commit()
        return len(rows)

    def upsert_term_summary(self, *, table: str, rows: List[dict]) -> int:
        if not rows:
            return 0

        hook = self._mysql_hook()
        conn = hook.get_conn()
        cols = ["dag_id", "owner", "trigger_date", "group_name", "term", "department", "match_count"]
        placeholders = ", ".join(["%s"] * len(cols))
        col_sql = ", ".join([f"`{c}`" for c in cols])

        sql = f"""
        INSERT INTO `{table}` ({col_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          `match_count` = VALUES(`match_count`)
        """

        values = [tuple(r.get(c) for c in cols) for r in rows]
        with conn.cursor() as cur:
            cur.executemany(sql, values)
        conn.commit()
        return len(rows)


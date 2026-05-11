from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional


SearchResult = Dict[str, Dict[str, Dict[str, List[dict]]]]


@dataclass(frozen=True)
class FindingRow:
    finding_hash: str
    dag_id: str
    owner: str
    run_id: str
    logical_date: datetime
    trigger_date: date
    source: Optional[str]
    search_header: Optional[str]
    group_name: str
    term: str
    department: str
    section: Optional[str]
    pubtype: Optional[str]
    title: Optional[str]
    href: Optional[str]
    publication_date: Optional[str]
    publication_id: Optional[str]
    hierarchy: Optional[str]
    ai_generated: Optional[bool]


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        # Accept YYYY-MM-DD (Airflow param) and DD/MM/YYYY (report values)
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _guess_source(section: Optional[str]) -> Optional[str]:
    if not section:
        return None
    if section.startswith("QD -"):
        return "QD"
    if section.startswith("DOU -") or section.startswith("DO"):
        return "DOU"
    return None


def _stable_hash(parts: Iterable[Optional[str]]) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_result_hash(row: FindingRow | dict) -> str:
    """Build a stable result hash independent of execution context.

    The hash intentionally excludes run-specific fields such as run_id,
    logical_date, trigger_date and search_index so the same publication can be
    compared across executions.
    """
    if isinstance(row, dict):
        publication_id = row.get("publication_id")
        href = row.get("href")
        title = row.get("title")
        publication_date = row.get("publication_date")
    else:
        publication_id = row.publication_id
        href = row.href
        title = row.title
        publication_date = row.publication_date

    return _stable_hash(
        [
            None if publication_id is None else str(publication_id),
            None if href is None else str(href),
            None if title is None else str(title),
            None if publication_date is None else str(publication_date),
        ]
    )


def extract_findings(
    *,
    search_payload: dict,
    dag_id: str,
    owner: str,
    run_id: str,
    logical_date: datetime,
    trigger_date: date,
    search_index: int,
) -> List[FindingRow]:
    """
    Convert one `exec_search_*` XCom payload into flat finding rows.

    The payload shape is produced by `DouDigestDagGenerator.perform_searches`:
      {
        "result": {group -> term -> department -> [items]},
        "header": "...",
        "department": [...],
        "department_ignore": [...],
        "pubtype": [...],
      }
    """
    header = search_payload.get("header")
    result: SearchResult = search_payload.get("result") or {}

    out: List[FindingRow] = []
    for group_name, group in result.items():
        if not isinstance(group, dict):
            continue
        for term, dept_map in group.items():
            # `dept_map` is expected to be a dict like {"single_department": [..]}
            if not isinstance(dept_map, dict):
                continue
            for department, items in dept_map.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    section = item.get("section")
                    href = item.get("href")
                    title = item.get("title")
                    pub_date = item.get("date")
                    pub_id = item.get("id")
                    hierarchy = item.get("hierarchyList")
                    pubtype = item.get("arttype")
                    ai_generated = item.get("ai_generated")

                    # Normalize mixed types to strings where appropriate
                    pub_id_s = None if pub_id is None else str(pub_id)
                    hierarchy_s = None
                    if hierarchy is not None:
                        hierarchy_s = (
                            " / ".join(hierarchy)
                            if isinstance(hierarchy, list)
                            else str(hierarchy)
                        )

                    finding_hash = _stable_hash(
                        [
                            dag_id,
                            owner,
                            str(trigger_date),
                            str(run_id),
                            str(search_index),
                            group_name,
                            term,
                            department,
                            str(pub_id_s),
                            str(href),
                            str(title),
                            str(pub_date),
                        ]
                    )

                    out.append(
                        FindingRow(
                            finding_hash=finding_hash,
                            dag_id=dag_id,
                            owner=owner,
                            run_id=run_id,
                            logical_date=logical_date,
                            trigger_date=trigger_date,
                            source=_guess_source(section),
                            search_header=header,
                            group_name=str(group_name),
                            term=str(term),
                            department=str(department),
                            section=None if section is None else str(section),
                            pubtype=None if pubtype is None else str(pubtype),
                            title=None if title is None else str(title),
                            href=None if href is None else str(href),
                            publication_date=None if pub_date is None else str(pub_date),
                            publication_id=pub_id_s,
                            hierarchy=hierarchy_s,
                            ai_generated=(
                                None if ai_generated is None else bool(ai_generated)
                            ),
                        )
                    )

    return out


def summarize_findings(rows: List[FindingRow]) -> List[dict]:
    """
    Produce a per-term summary suitable for upsert:
      (dag_id, owner, trigger_date, group_name, term, department) -> match_count
    """
    counts: Dict[tuple, int] = {}
    for r in rows:
        key = (r.dag_id, r.owner, r.trigger_date, r.group_name, r.term, r.department)
        counts[key] = counts.get(key, 0) + 1

    return [
        {
            "dag_id": k[0],
            "owner": k[1],
            "trigger_date": k[2],
            "group_name": k[3],
            "term": k[4],
            "department": k[5],
            "match_count": v,
        }
        for k, v in counts.items()
    ]


from datetime import datetime, date

from analytics.extractor import extract_findings, summarize_findings


def test_extract_findings_flattens_nested_result():
    payload = {
        "header": "Header A",
        "result": {
            "single_group": {
                "Bar": {
                    "single_department": [
                        {
                            "section": "DOU - Seção 1",
                            "title": "PORTARIA X",
                            "href": "https://example.gov.br/doc/1",
                            "abstract": "....",
                            "date": "08/05/2026",
                            "id": 123,
                            "hierarchyList": ["Ministério A", "Secretaria B"],
                            "arttype": "Portaria",
                            "ai_generated": False,
                        }
                    ]
                }
            }
        },
        "department": None,
        "pubtype": ["Portaria"],
    }

    rows = extract_findings(
        search_payload=payload,
        dag_id="dag_x",
        owner="andre",
        run_id="manual__1",
        logical_date=datetime(2026, 5, 8, 9, 0, 0),
        trigger_date=date(2026, 5, 8),
        search_index=1,
    )

    assert len(rows) == 1
    r = rows[0]
    assert r.term == "Bar"
    assert r.department == "single_department"
    assert r.section == "DOU - Seção 1"
    assert r.pubtype == "Portaria"
    assert r.hierarchy == "Ministério A / Secretaria B"
    assert r.source == "DOU"


def test_summarize_findings_counts_per_term_department():
    payload = {
        "header": None,
        "result": {
            "single_group": {
                "Bar": {"d1": [{"section": "DOU - Seção 1"}, {"section": "DOU - Seção 1"}]},
                "Baz": {"d1": [{"section": "QD - Edição ordinária"}]},
            }
        },
    }

    rows = extract_findings(
        search_payload=payload,
        dag_id="dag_x",
        owner="andre",
        run_id="manual__1",
        logical_date=datetime(2026, 5, 8, 9, 0, 0),
        trigger_date=date(2026, 5, 8),
        search_index=1,
    )

    summary = summarize_findings(rows)
    summary_map = {
        (s["group_name"], s["term"], s["department"]): s["match_count"] for s in summary
    }
    assert summary_map[("single_group", "Bar", "d1")] == 2
    assert summary_map[("single_group", "Baz", "d1")] == 1


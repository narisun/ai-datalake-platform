"""Spike: print what OpenMetadata now knows. Run inside the ingestion container."""

from __future__ import annotations

import json
import pathlib
import sys
from urllib.parse import quote

import requests

API = "http://openmetadata-server:8585/api/v1"
TOKEN = pathlib.Path("/opt/datalake/work/om_token").read_text().strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def get(path: str, **params: object) -> dict:
    response = requests.get(f"{API}/{path}", headers=HEADERS, params=params, timeout=30)
    if response.status_code >= 400:
        return {"_error": f"{response.status_code} {response.text[:200]}"}
    return response.json()


def table(fqn: str) -> None:
    data = get(
        f"tables/name/{quote(fqn, safe='')}",
        fields="tags,owners,domains,dataModel,columns,testSuite",
    )
    if "_error" in data:
        print(f"TABLE {fqn}: {data['_error']}")
        return
    print(f"TABLE {fqn}  type={data.get('tableType')}")
    print("  description:", (data.get("description") or "")[:70])
    print("  owners:", [o.get("name") for o in data.get("owners") or []])
    print("  domains:", [d.get("name") for d in data.get("domains") or []])
    print("  tags:", [t.get("tagFQN") for t in data.get("tags") or []])
    model = data.get("dataModel") or {}
    print("  dbt model:", model.get("modelType"), "sql" if model.get("sql") else "no-sql")
    for column in data.get("columns") or []:
        tags = [t.get("tagFQN") for t in column.get("tags") or []]
        if tags or column.get("description"):
            print(
                f"    {column['name']}: tags={tags} desc={(column.get('description') or '')[:40]!r}"
            )

    lineage = get(f"lineage/table/name/{quote(fqn, safe='')}", upstreamDepth=3, downstreamDepth=3)
    names = {
        n["id"]: f"{n['type']}:{n.get('fullyQualifiedName')}" for n in lineage.get("nodes", [])
    }
    names[lineage.get("entity", {}).get("id")] = f"table:{fqn}"
    for direction in ("upstreamEdges", "downstreamEdges"):
        for edge in lineage.get(direction, []):
            details = edge.get("lineageDetails") or {}
            via = (details.get("pipeline") or {}).get("fullyQualifiedName")
            cols = len(details.get("columnsLineage") or [])
            print(
                f"  {direction[:-5]}: {names.get(edge['fromEntity'])} -> {names.get(edge['toEntity'])}"
                f"  source={details.get('source')} pipeline={via} column_edges={cols}"
            )


def tests() -> None:
    data = get("dataQuality/testCases", limit=50, fields="testCaseResult")
    cases = data.get("data", [])
    print(f"TEST CASES: {len(cases)}")
    for case in cases[:12]:
        result = (case.get("testCaseResult") or {}).get("testCaseStatus")
        print(f"  {case['name'][:60]}: {result}")


def pipelines() -> None:
    data = get("pipelines", limit=50, fields="tasks,pipelineStatus,scheduleInterval,owners")
    for pipeline in data.get("data", []):
        print(f"PIPELINE {pipeline['fullyQualifiedName']}")
        print("  scheduleInterval:", pipeline.get("scheduleInterval"))
        print("  sourceUrl:", pipeline.get("sourceUrl"))
        print("  tasks:", [t["name"] for t in pipeline.get("tasks") or []])
        status = pipeline.get("pipelineStatus") or {}
        print(
            "  last status:",
            status.get("executionStatus"),
            [(t.get("name"), t.get("executionStatus")) for t in status.get("taskStatus") or []],
        )
        lineage = get(
            f"lineage/pipeline/name/{quote(pipeline['fullyQualifiedName'], safe='')}",
            upstreamDepth=1,
            downstreamDepth=1,
        )
        names = {n["id"]: n.get("fullyQualifiedName") for n in lineage.get("nodes", [])}
        names[pipeline["id"]] = "<this pipeline>"
        for direction in ("upstreamEdges", "downstreamEdges"):
            for edge in lineage.get(direction, []):
                print(
                    f"  {direction[:-5]}: {names.get(edge['fromEntity'])} -> {names.get(edge['toEntity'])}"
                )


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        if arg == "tests":
            tests()
        elif arg == "pipelines":
            pipelines()
        elif arg == "version":
            print(json.dumps(get("system/version")))
        else:
            table(arg)

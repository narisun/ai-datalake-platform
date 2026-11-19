"""Chunk building, the catalog and search adapters (over a fake HTTP transport), and
the index process."""

# pyright: reportUnknownLambdaType=false

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from datalake.catalog.model import CatalogColumn, CatalogTable, GlossaryTerm
from datalake.catalog.openmetadata import CatalogError, OpenMetadataCatalog
from datalake.catalog.testing import InMemoryCatalogReader
from datalake.control import ConfigResolver, Control
from datalake.control.runtime import ChainedSecrets, EnvironmentSecrets, FileSecrets
from datalake.control.testing import (
    InMemoryControlStore,
    SequentialUuids,
    StaticSecrets,
    TickingClock,
)
from datalake.core.security import Classification
from datalake.policy.model import SearchFilter
from datalake.search.chunks import build_chunks
from datalake.search.indexing import IndexDependencies, SearchSettings, run_index
from datalake.search.model import Query, SearchHit
from datalake.search.opensearch import OpenSearchChunkIndex, VectorSearch, scope_filter
from datalake.search.tei import ModelServiceError, TeiEmbedder, TeiReranker
from datalake.search.testing import HashingEmbedder, InMemoryLexicalIndex
from tests.unit.helpers import chunk

INTERNAL, CONFIDENTIAL = Classification.INTERNAL, Classification.CONFIDENTIAL

CUSTOMER = CatalogTable(
    fqn="lake_trino.lake.marts.dim_customer",
    sql_name="lake.marts.dim_customer",
    name="dim_customer",
    schema="marts",
    description="One row per customer.",
    domain="sales",
    classification=INTERNAL,
    columns=(
        CatalogColumn("customer_id", "varchar", "Customer identifier.", INTERNAL),
        CatalogColumn("email", "varchar", "Email address.", CONFIDENTIAL),
    ),
    glossary_terms=("Business.Customer",),
)
PAYROLL = CatalogTable(
    fqn="lake_trino.lake.marts.payroll",
    sql_name="lake.marts.payroll",
    name="payroll",
    schema="marts",
    domain="hr",
    classification=INTERNAL,
    glossary_terms=("Business.Customer",),
)
UNTAGGED = CatalogTable(
    fqn="lake_trino.lake.raw.orders", sql_name="lake.raw.orders", name="orders", schema="raw"
)
TERM = GlossaryTerm("Business.Customer", "Customer", "Someone who buys.", ("client",))


# --- chunks ---------------------------------------------------------------------------


def test_table_chunk_leaves_out_columns_above_the_tables_classification() -> None:
    by_id = {c.chunk_id: c for c in build_chunks([CUSTOMER], [TERM])}

    table = by_id["table:lake_trino.lake.marts.dim_customer"]
    assert table.domain == "sales"
    assert table.classification is INTERNAL
    assert "Table lake.marts.dim_customer" in table.text
    assert "customer_id (varchar): Customer identifier." in table.text
    assert "Business term: Customer (also called: client). Someone who buys." in table.text
    assert "email" not in table.text

    sensitive = by_id["table:lake_trino.lake.marts.dim_customer#confidential"]
    assert sensitive.classification is CONFIDENTIAL
    assert "email (varchar): Email address." in sensitive.text


def test_untagged_table_is_not_indexed_at_all() -> None:
    assert build_chunks([UNTAGGED], []) == []


def test_a_table_in_a_hidden_schema_is_not_indexed_and_nor_are_its_terms() -> None:
    assert build_chunks([CUSTOMER], [TERM], frozenset({CUSTOMER.schema})) == []


def test_term_is_indexed_per_domain_and_names_only_that_domains_tables() -> None:
    by_id = {c.chunk_id: c for c in build_chunks([CUSTOMER, PAYROLL], [TERM])}

    sales = by_id["term:Business.Customer@sales"]
    hr = by_id["term:Business.Customer@hr"]
    assert sales.classification is Classification.PUBLIC
    assert "also called: client" in sales.text
    assert "lake.marts.dim_customer" in sales.text
    assert "payroll" not in sales.text
    assert "lake.marts.payroll" in hr.text
    assert "dim_customer" not in hr.text


def test_content_hash_changes_with_text_domain_or_classification() -> None:
    base = build_chunks([CUSTOMER], [])[0].content_hash
    from dataclasses import replace

    assert build_chunks([CUSTOMER], [])[0].content_hash == base
    for changed in (
        replace(CUSTOMER, description="Changed."),
        replace(CUSTOMER, domain="finance"),
        replace(CUSTOMER, classification=CONFIDENTIAL),
    ):
        assert build_chunks([changed], [])[0].content_hash != base


# --- OpenMetadata ---------------------------------------------------------------------


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_openmetadata_tables_are_paged_filtered_and_mapped() -> None:
    seen: list[httpx.Request] = []
    table = {
        "fullyQualifiedName": "lake_trino.lake.marts.dim_customer",
        "name": "dim_customer",
        "description": "One row per customer.",
        "databaseSchema": {"name": "marts"},
        "domains": [{"name": "sales"}],
        "owners": [{"name": "sales-data"}],
        "tags": [
            {"tagFQN": "dbtTags.internal", "source": "Classification"},
            {"tagFQN": "Business.Customer", "source": "Glossary"},
            {"tagFQN": "Tier.Tier2", "source": "Classification"},
        ],
        "columns": [
            {"name": "customer_id", "dataTypeDisplay": "varchar"},
            {"name": "email", "tags": [{"tagFQN": "dbtTags.confidential"}]},
        ],
    }
    other_service = {"fullyQualifiedName": "shop_db.shop.public.orders", "name": "orders"}
    untagged = {"fullyQualifiedName": "lake_trino.lake.raw.orders", "name": "orders"}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "after" not in request.url.params:
            return httpx.Response(
                200, json={"data": [table, other_service], "paging": {"after": "p2"}}
            )
        return httpx.Response(200, json={"data": [untagged], "paging": {}})

    catalog = OpenMetadataCatalog(_client(handler), "http://om/api", "tok", "lake_trino")
    first, second = await catalog.tables()

    assert len(seen) == 2
    assert seen[0].headers["authorization"] == "Bearer tok"
    assert seen[0].url.path == "/api/v1/tables"
    assert (first.sql_name, first.schema, first.domain, first.owner) == (
        "lake.marts.dim_customer",
        "marts",
        "sales",
        "sales-data",
    )
    assert first.classification is INTERNAL
    assert first.glossary_terms == ("Business.Customer",)
    assert [c.classification for c in first.columns] == [INTERNAL, CONFIDENTIAL]
    # No tags at all: hidden by default.
    assert second.domain == ""
    assert second.classification is Classification.RESTRICTED


async def test_openmetadata_terms_and_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/glossaryTerms"):
            term = {
                "fullyQualifiedName": "Business.Customer",
                "name": "Customer",
                "synonyms": ["client"],
            }
            return httpx.Response(200, json={"data": [term]})
        return httpx.Response(401, json={"message": "no"})

    catalog = OpenMetadataCatalog(_client(handler), "http://om/api", "tok", "lake_trino")

    (term,) = await catalog.glossary_terms()
    assert (term.fqn, term.name, term.synonyms) == ("Business.Customer", "Customer", ("client",))
    with pytest.raises(CatalogError, match="cannot read tables"):
        await catalog.tables()


# --- OpenSearch -----------------------------------------------------------------------


def test_scope_filter_shapes() -> None:
    assert scope_filter(None) is None
    empty = scope_filter(SearchFilter({}))
    assert empty == {"bool": {"should": [], "minimum_should_match": 1}}  # matches nothing
    scoped = scope_filter(SearchFilter({"sales": CONFIDENTIAL}))
    assert scoped is not None
    assert scoped["bool"]["should"] == [
        {
            "bool": {
                "filter": [
                    {"term": {"domain": "sales"}},
                    {"range": {"classification": {"lte": 2}}},
                ]
            }
        }
    ]


class FakeOpenSearch:
    def __init__(self, exists: bool = True) -> None:
        self.exists = exists
        self.requests: list[tuple[str, str, Any]] = []
        self.hits: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body: Any = None
        if request.content:
            text = request.content.decode()
            body = (
                [json.loads(line) for line in text.splitlines()]
                if "_bulk" in request.url.path
                else json.loads(text)
            )
        self.requests.append((request.method, request.url.path, body))
        if request.method == "HEAD":
            return httpx.Response(200 if self.exists else 404)
        if request.method == "PUT":
            self.exists = True
            return httpx.Response(200, json={})
        if request.url.path.endswith("/_bulk"):
            return httpx.Response(200, json={"errors": False, "items": []})
        return httpx.Response(200, json={"hits": {"hits": self.hits}})


def _index(fake: FakeOpenSearch) -> OpenSearchChunkIndex:
    return OpenSearchChunkIndex(_client(fake), "http://os:9200", "chunks", HashingEmbedder(8))


async def test_keyword_and_vector_search_carry_the_callers_scope() -> None:
    fake = FakeOpenSearch()
    fake.hits = [
        {
            "_id": "table:x",
            "_score": 1.5,
            "_source": {
                "entity_urn": "x",
                "text": "Table x",
                "domain": "sales",
                "classification": 1,
            },
        }
    ]
    index = _index(fake)
    query = Query("revenue").scoped_to(SearchFilter({"sales": INTERNAL}))

    (hit,) = await index.search(query, 5)
    await VectorSearch(index).search(query, 5)

    assert (hit.chunk.chunk_id, hit.score, hit.chunk.classification) == ("table:x", 1.5, INTERNAL)
    keyword, vector = fake.requests[0][2], fake.requests[1][2]
    expected = scope_filter(query.scope)
    assert keyword["query"]["bool"]["must"] == [{"match": {"text": "revenue"}}]
    assert keyword["query"]["bool"]["filter"] == [expected]
    assert vector["query"]["knn"]["embedding"]["filter"] == expected
    assert vector["query"]["knn"]["embedding"]["k"] == 5
    assert len(vector["query"]["knn"]["embedding"]["vector"]) == 8
    assert "embedding" not in keyword["_source"]


async def test_upsert_creates_the_index_with_the_models_vector_size() -> None:
    fake = FakeOpenSearch(exists=False)

    await _index(fake).upsert([chunk("a", "revenue by order")])

    methods = [(m, p) for m, p, _ in fake.requests]
    assert methods == [("HEAD", "/chunks"), ("PUT", "/chunks"), ("POST", "/_bulk")]
    mapping = fake.requests[1][2]["mappings"]["properties"]
    assert mapping["embedding"]["dimension"] == 8
    action, document = fake.requests[2][2]
    assert action == {"index": {"_index": "chunks", "_id": "a"}}
    assert (document["domain"], document["classification"]) == ("sales", 1)
    assert len(document["embedding"]) == 8


async def test_delete_and_missing_index() -> None:
    fake = FakeOpenSearch()
    index = _index(fake)

    await index.delete(["a", "b"])
    await index.delete([])

    assert len(fake.requests) == 1
    assert fake.requests[0][2] == [
        {"delete": {"_index": "chunks", "_id": "a"}},
        {"delete": {"_index": "chunks", "_id": "b"}},
    ]

    missing = OpenSearchChunkIndex(
        _client(lambda request: httpx.Response(404, json={})),
        "http://os",
        "chunks",
        HashingEmbedder(8),
    )
    assert await missing.search(Query("x"), 5) == []
    assert await missing.content_hashes() == {}


# --- embedding and reranking ----------------------------------------------------------


async def test_embedder_batches_and_reranker_orders_by_score() -> None:
    calls: list[tuple[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append((request.url.path, body))
        if request.url.path == "/embed":
            return httpx.Response(200, json=[[0.1, 0.2] for _ in body["inputs"]])
        return httpx.Response(200, json=[{"index": 0, "score": 0.2}, {"index": 1, "score": 0.9}])

    http = _client(handler)
    vectors = await TeiEmbedder(http, "http://tei").embed([f"text {n}" for n in range(20)])
    hits = [SearchHit(chunk("low"), 9.0), SearchHit(chunk("high"), 1.0)]
    passages = await TeiReranker(http, "http://tei").rerank(Query("q"), hits, top_n=1)

    assert len(vectors) == 20
    assert [len(body["inputs"]) for path, body in calls if path == "/embed"] == [16, 4]
    assert [(p.chunk.chunk_id, p.score) for p in passages] == [("high", 0.9)]
    assert calls[-1][1]["query"] == "q"
    assert await TeiReranker(http, "http://tei").rerank(Query("q"), [], top_n=3) == []


async def test_model_service_failure_is_reported() -> None:
    http = _client(lambda request: httpx.Response(503))

    with pytest.raises(ModelServiceError):
        await TeiEmbedder(http, "http://tei").embed(["x"])


# --- the index process ----------------------------------------------------------------


def _control(store: InMemoryControlStore) -> Control:
    return Control(
        store=store,
        resolver=ConfigResolver({}, StaticSecrets({"om": "tok"}), strict=True),
        clock=TickingClock(),
        ids=SequentialUuids(),
        environment={},
    )


async def test_index_writes_changes_and_removes_what_is_gone() -> None:
    store = InMemoryControlStore()
    store.register_process("index")
    for key, value in {
        "access_policy_file": "access.yml",
        "openmetadata_url": "http://om/api",
        "openmetadata_token": "${secret:om}",
        "opensearch_url": "http://os",
        "embed_url": "http://embed",
        "rerank_url": "http://rerank",
    }.items():
        store.set_config(key, value, None, "tester")
    catalog = InMemoryCatalogReader([CUSTOMER, PAYROLL], [TERM])
    index = InMemoryLexicalIndex()
    seen: list[SearchSettings] = []

    def build(settings: SearchSettings) -> IndexDependencies:
        seen.append(settings)
        return IndexDependencies(catalog, index)

    first = await run_index(_control(store), build)
    assert (first.total, first.written, first.deleted, first.unchanged) == (5, 5, 0, 0)
    assert seen[0].openmetadata_token == "tok"

    second = await run_index(_control(store), build)
    assert (second.written, second.deleted, second.unchanged) == (0, 0, 5)

    # Payroll is dropped, and the customer table becomes confidential.
    from dataclasses import replace

    catalog.table_list = [replace(CUSTOMER, classification=CONFIDENTIAL)]
    third = await run_index(_control(store), build)

    hashes = await index.content_hashes()
    assert set(hashes) == {
        "table:lake_trino.lake.marts.dim_customer",
        "term:Business.Customer@sales",
    }
    assert (third.written, third.deleted) == (1, 3)
    found = await index.search(Query("customer").scoped_to(SearchFilter({"sales": INTERNAL})), 5)
    assert [h.chunk.chunk_id for h in found] == ["term:Business.Customer@sales"]


# --- secrets from files ---------------------------------------------------------------


def test_secrets_come_from_the_environment_then_from_files(tmp_path: Any) -> None:
    (tmp_path / "openmetadata-token").write_text("from-file\n", encoding="utf-8")
    secrets = ChainedSecrets(
        [EnvironmentSecrets({"DL_SECRET_TRINO_PASSWORD": "from-env"}), FileSecrets(tmp_path)]
    )

    assert secrets.get("trino-password") == "from-env"
    assert secrets.get("openmetadata-token") == "from-file"
    with pytest.raises(Exception, match="not found"):
        secrets.get("missing")
    with pytest.raises(Exception, match="not allowed"):
        FileSecrets(tmp_path).get("../etc/passwd")

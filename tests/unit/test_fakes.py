"""The fakes are shipped to SDK users, so their behaviour is tested too."""

from __future__ import annotations

import pytest

from datalake.agent.model import AgentEvent, ChatMessage, QueryResult
from datalake.agent.testing import FakeQueryEngine, ScriptedAgentRuntime, ScriptedChatModel
from datalake.catalog.testing import InMemoryMetadataCatalog
from datalake.core.catalog import Dataset, LineageEdge, Term
from datalake.core.errors import NotFoundError
from datalake.core.security import Principal
from datalake.ingest.model import IngestResult
from datalake.ingest.testing import FakeIngestor
from datalake.search.model import Query
from datalake.search.testing import HashingEmbedder, InMemoryLexicalIndex, InMemoryVectorIndex
from datalake.storage.testing import InMemoryObjectStore, InMemoryTableCatalog
from datalake.transform.testing import FakeTransformEngine
from tests.unit.helpers import chunk


async def test_object_store_round_trip() -> None:
    store = InMemoryObjectStore()
    await store.put("raw/a.parquet", b"a")
    await store.put("raw/b.parquet", b"b")
    await store.put("marts/c.parquet", b"c")

    assert await store.get("raw/a.parquet") == b"a"
    assert await store.list("raw/") == ["raw/a.parquet", "raw/b.parquet"]

    await store.delete("raw/a.parquet")
    await store.delete("raw/a.parquet")
    assert not await store.exists("raw/a.parquet")
    with pytest.raises(NotFoundError):
        await store.get("raw/a.parquet")


async def test_table_catalog() -> None:
    catalog = InMemoryTableCatalog({"raw": ["orders"]})
    catalog.add_table("marts", "fct_orders")

    assert await catalog.list_namespaces() == ["marts", "raw"]
    assert await catalog.list_tables("raw") == ["orders"]
    assert await catalog.table_exists("marts", "fct_orders")
    assert not await catalog.table_exists("marts", "missing")


async def test_metadata_catalog_lineage_and_lookup() -> None:
    catalog = InMemoryMetadataCatalog()
    await catalog.upsert_datasets(
        [Dataset("urn:stg", "stg_orders", "sales"), Dataset("urn:fct", "fct_orders", "sales")]
    )
    await catalog.upsert_terms([Term("Customer", synonyms=("client",))])
    await catalog.add_lineage([LineageEdge("urn:stg", "urn:fct")])

    assert (await catalog.get_dataset("urn:fct")).name == "fct_orders"
    assert [d.urn for d in await catalog.list_datasets()] == ["urn:fct", "urn:stg"]
    assert (await catalog.get_term("Customer")).synonyms == ("client",)
    assert await catalog.upstream("urn:fct") == ["urn:stg"]
    assert await catalog.downstream("urn:stg") == ["urn:fct"]
    with pytest.raises(NotFoundError):
        await catalog.get_dataset("urn:none")
    with pytest.raises(NotFoundError):
        await catalog.get_term("Nope")


async def test_indexes_rank_upsert_and_delete() -> None:
    lexical = InMemoryLexicalIndex()
    vector = InMemoryVectorIndex(HashingEmbedder())
    chunks = [chunk("a", "revenue revenue"), chunk("b", "revenue"), chunk("c", "weather")]
    for index in (lexical, vector):
        await index.upsert(chunks)

    assert [h.chunk.chunk_id for h in await lexical.search(Query("revenue"), 10)] == ["a", "b"]
    assert {h.chunk.chunk_id for h in await vector.search(Query("revenue"), 10)} == {"a", "b"}
    assert len(await lexical.search(Query("revenue"), 1)) == 1

    for index in (lexical, vector):
        await index.delete(["a"])
        assert set(await index.content_hashes()) == {"b", "c"}
    assert [h.chunk.chunk_id for h in await vector.search(Query("revenue"), 10)] == ["b"]


async def test_pipeline_fakes_record_calls() -> None:
    ingestor = FakeIngestor({"crm": IngestResult("crm", ("customers",), 10)})
    engine = FakeTransformEngine(models=["stg_orders", "fct_orders"])

    assert (await ingestor.ingest("crm")).rows_loaded == 10
    with pytest.raises(NotFoundError):
        await ingestor.ingest("unknown")
    assert (await engine.build()).models_built == ("stg_orders", "fct_orders")
    assert (await engine.build("fct_orders")).models_built == ("fct_orders",)
    assert ingestor.ingested == ["crm", "unknown"]
    assert engine.selections == [None, "fct_orders"]


async def test_query_engine_truncates_and_records() -> None:
    engine = FakeQueryEngine(QueryResult(("n",), ((1,), (2,), (3,))))

    result = await engine.execute("select n from t", role="analyst_sales", max_rows=2)

    assert result.rows == ((1,), (2,))
    assert result.truncated
    assert engine.executed[0].role == "analyst_sales"


async def test_scripted_chat_model_and_agent() -> None:
    model = ScriptedChatModel(["first"])
    assert await model.complete([ChatMessage("user", "hi")]) == "first"
    with pytest.raises(AssertionError):
        await model.complete([ChatMessage("user", "again")])

    agent = ScriptedAgentRuntime([AgentEvent("status", "thinking"), AgentEvent("final", "42")])
    events = [e async for e in agent.ask("why?", Principal("alice"), "t1")]
    assert [e.kind for e in events] == ["status", "final"]
    assert agent.questions == [("why?", "alice", "t1")]

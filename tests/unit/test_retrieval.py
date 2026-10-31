"""Paper-aligned retrieval unit tests, including real HTTP adapter contract."""

import asyncio
import json
import math

import httpx
import pytest

from mcp_gateway.retrieval import (
    CANDIDATE_LIMIT,
    DeterministicEmbedding,
    EmbeddingError,
    HTTPEmbeddingProvider,
    HybridRetriever,
    reciprocal_rank_fusion,
    tokenize,
    tool_text,
)


def tool(name, description="", properties=None):
    return {
        "name": name,
        "description": description,
        "inputSchema": {"type": "object", "properties": properties or {}},
    }


class FixedEmbedding:
    def __init__(self, vectors=None):
        self.calls = []
        self.vectors = vectors or {}

    async def embed(self, texts):
        self.calls.append(list(texts))
        return [self.vectors.get(text, [1.0, 0.0]) for text in texts]


def test_tokenization_splits_acronyms_and_camel_case():
    assert tokenize("the getHTTPServer_ByResourceID and café 实例") == [
        "get",
        "http",
        "server",
        "resource",
        "id",
        "café",
        "实例",
    ]


def test_index_text_excludes_nested_schemas_and_examples():
    definition = tool(
        "compute__listInstances",
        "Find resources",
        {
            "regionId": {"type": "string", "description": "TOP_SECRET_DEFAULT"},
            "filters": {"type": "object", "properties": {"hiddenNested": {"type": "string"}}},
        },
    )
    definition["examples"] = ["secretExample"]
    assert tool_text(definition) == "compute__listInstances Find resources regionId filters"
    assert "TOP_SECRET" not in tool_text(definition)
    assert "hiddenNested" not in tool_text(definition)


def test_rrf_exact_outer_join_and_deterministic_ties():
    ranked = reciprocal_rank_fusion([["a", "b"], ["c", "a"]])
    assert [name for name, _ in ranked] == ["a", "c", "b"]
    assert ranked[0][1] == pytest.approx(1 / 21 + 1 / 22)
    assert ranked[1][1] == pytest.approx(1 / 21)
    assert reciprocal_rank_fusion([["z"], ["a"]]) == [("a", 1 / 21), ("z", 1 / 21)]
    with pytest.raises(ValueError, match="duplicate"):
        reciprocal_rank_fusion([["a", "a"]])
    with pytest.raises(ValueError):
        reciprocal_rank_fusion([["a"]], k=-1)


@pytest.mark.asyncio
async def test_offline_fallback_is_deterministic_and_explicitly_nonsemantic():
    first, second = DeterministicEmbedding(), DeterministicEmbedding()
    assert first.semantic is False
    assert await first.embed(["list virtual machines"]) == await second.embed(["list virtual machines"])
    assert len((await first.embed([""]))[0]) == 384
    with pytest.raises(ValueError):
        DeterministicEmbedding(dimensions=True)


@pytest.mark.asyncio
async def test_keyword_parameter_retrieval_and_default_limit():
    definitions = [tool(f"tool{i}", f"description {i}") for i in range(20)]
    definitions.append(tool("compute__fetch", "Retrieve instances", {"uniqueResourceId": {"type": "string"}}))
    retriever = HybridRetriever(definitions)
    results = await retriever.search("uniqueResourceId", {item["name"] for item in definitions})
    assert results[0]["tool"]["name"] == "compute__fetch"
    assert len(results) == 15
    assert all(math.isfinite(result["score"]) and result["score"] > 0 for result in results)


@pytest.mark.asyncio
async def test_acl_is_applied_before_rankings_not_filtered_after_top_k():
    definitions = [
        tool("aaa_secret", "delete sensitive database"),
        tool("zzz_allowed", "list public catalog"),
    ]
    provider = FixedEmbedding()
    retriever = HybridRetriever(definitions, provider)
    result = await retriever.search("delete sensitive database", {"zzz_allowed"}, top_k=1)
    assert [item["tool"]["name"] for item in result] == ["zzz_allowed"]
    assert "secret" not in json.dumps(result)
    empty_provider = FixedEmbedding()
    assert await HybridRetriever(definitions, empty_provider).search("secret", set()) == []
    assert empty_provider.calls == []


@pytest.mark.asyncio
async def test_acl_filtered_corpus_statistics_cannot_change_public_results():
    public = [tool("public_a", "list image image"), tool("public_b", "list image resource snapshot volume")]
    hidden = [tool(f"hidden{i}", "list image image image") for i in range(20)]
    baseline = HybridRetriever(public, FixedEmbedding())
    combined = HybridRetriever(public + hidden, FixedEmbedding())
    allowed = {"public_a", "public_b"}
    assert await baseline.search("image", allowed) == await combined.search("image", allowed)


@pytest.mark.asyncio
async def test_results_and_input_are_defensively_copied():
    definition = tool("public", "original", {"id": {"type": "string"}})
    retriever = HybridRetriever([definition])
    definition["description"] = "mutated before first request"
    first = await retriever.search("public", {"public"})
    first[0]["tool"]["inputSchema"]["properties"]["secret"] = {"type": "string"}
    second = await retriever.search("public", {"public"})
    assert second[0]["tool"]["description"] == "original"
    assert "secret" not in second[0]["tool"]["inputSchema"]["properties"]


@pytest.mark.asyncio
async def test_dense_cosine_normalization_not_dot_product():
    definitions = [tool("far"), tool("near")]
    provider = FixedEmbedding({"far ": [10000.0, 10000.0], "near ": [2.0, 0.0], "semantic": [1.0, 0.0]})
    result = await HybridRetriever(definitions, provider).search("semantic", {"far", "near"})
    assert [item["tool"]["name"] for item in result] == ["near", "far"]
    assert result[0]["score"] == pytest.approx(1 / 21)  # No sparse matches.


@pytest.mark.asyncio
async def test_build_is_once_under_concurrent_searches():
    provider = FixedEmbedding()
    retriever = HybridRetriever([tool("list")], provider)
    await asyncio.gather(*(retriever.search("query", {"list"}) for _ in range(5)))
    assert sum(call == ["list "] for call in provider.calls) == 1
    assert len(provider.calls) == 6


@pytest.mark.asyncio
async def test_candidate_lists_are_bounded_to_120_before_rrf():
    definitions = [tool(f"tool_{index:03}") for index in range(300)]
    result = await HybridRetriever(definitions, FixedEmbedding()).search(
        "unmatched",
        {item["name"] for item in definitions},
        top_k=CANDIDATE_LIMIT,
    )
    assert len(result) == 120
    assert result[-1]["tool"]["name"] == "tool_119"


@pytest.mark.asyncio
async def test_empty_inputs_do_not_call_embedding_provider():
    provider = FixedEmbedding()
    empty = HybridRetriever([], provider)
    await empty.build()
    assert await empty.search("some query", {"missing"}) == []
    retriever = HybridRetriever([tool("public")], provider)
    assert await retriever.search("   ", {"public"}) == []
    assert await retriever.search("the and of", {"public"}) == []
    assert provider.calls == []


@pytest.mark.parametrize("top_k", [0, -1, 121, True, 1.5])
@pytest.mark.asyncio
async def test_invalid_top_k(top_k):
    with pytest.raises(ValueError, match="top_k"):
        await HybridRetriever([]).search("query", set(), top_k)


@pytest.mark.asyncio
async def test_invalid_query_or_acl():
    retriever = HybridRetriever([])
    for query in [None, "x" * 8193]:
        with pytest.raises(ValueError, match="query"):
            await retriever.search(query, set())
    with pytest.raises(ValueError, match="allowed_names"):
        await retriever.search("query", ["public"])


@pytest.mark.parametrize(
    "definitions",
    [
        [tool("same"), tool("same")],
        [{"name": ""}],
        [{"name": "x", "description": None}],
        [{"name": "x", "inputSchema": {"properties": []}}],
        [{"name": "x", "inputSchema": {"properties": {1: {}}}}],
    ],
)
def test_invalid_tool_metadata_fails_fast(definitions):
    with pytest.raises(ValueError):
        HybridRetriever(definitions)


@pytest.mark.parametrize(
    "vectors",
    [
        [],
        [[0.0, 0.0]],
        [[float("nan"), 1.0]],
        [[float("inf"), 1.0]],
        [[True, 1.0]],
        [["bad", 1.0]],
        [[]],
        [[10**500, 1]],
    ],
)
@pytest.mark.asyncio
async def test_invalid_index_embeddings_fail_atomically(vectors):
    class InvalidEmbedding:
        async def embed(self, texts):
            return vectors

    retriever = HybridRetriever([tool("public")], InvalidEmbedding())
    with pytest.raises(EmbeddingError):
        await retriever.build()
    assert retriever._vectors is None


@pytest.mark.asyncio
async def test_query_dimension_mismatch_fails_without_fallback():
    provider = FixedEmbedding({"public ": [1.0, 0.0], "query": [1.0, 0.0, 0.0]})
    with pytest.raises(EmbeddingError, match="dimensions"):
        await HybridRetriever([tool("public")], provider).search("query", {"public"})


@pytest.mark.asyncio
async def test_provider_failure_build_can_be_retried_without_partial_index():
    class RecoveringProvider:
        calls = 0

        async def embed(self, texts):
            self.calls += 1
            if self.calls == 1:
                raise EmbeddingError("temporary provider outage")
            return [[1.0, 0.0] for _ in texts]

    retriever = HybridRetriever([tool("public")], RecoveringProvider())
    with pytest.raises(EmbeddingError):
        await retriever.build()
    assert (await retriever.search("public", {"public"}))[0]["tool"]["name"] == "public"


@pytest.mark.asyncio
async def test_http_provider_batches_reorders_and_uses_separate_credentials():
    requests = []

    async def handler(request):
        payload = json.loads(request.content)
        requests.append((request, payload))
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": index, "embedding": [float(index + 1), 2.0]}
                    for index in reversed(range(len(payload["input"])))
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = HTTPEmbeddingProvider(
            "https://embedding.invalid/v1/embeddings",
            "a-real-model",
            api_key="backend-secret",
            batch_size=2,
            client=client,
        )
        vectors = await provider.embed(["one", "two", "three"])
        assert vectors == [[1.0, 2.0], [2.0, 2.0], [1.0, 2.0]]
        assert len(requests) == 2
        assert requests[0][0].headers["authorization"] == "Bearer backend-secret"
        assert requests[0][1] == {
            "input": ["one", "two"],
            "model": "a-real-model",
            "encoding_format": "float",
        }
        assert not client.is_closed
        assert await provider.embed([]) == []


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"data": {}},
        {"data": []},
        {"data": [42]},
        {"data": [{"index": 2, "embedding": [1]}]},
        {"data": [{"index": True, "embedding": [1]}]},
        {"data": [{"index": 0, "embedding": [0]}]},
        {"data": [{"index": 0, "embedding": [float("nan")]}]},
    ],
)
@pytest.mark.asyncio
async def test_http_provider_rejects_malformed_responses(payload):
    async def handler(request):
        return httpx.Response(200, content=json.dumps(payload), headers={"content-type": "application/json"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = HTTPEmbeddingProvider("http://embedding.invalid/embeddings", "model", client=client)
        with pytest.raises(EmbeddingError):
            await provider.embed(["query"])


@pytest.mark.asyncio
async def test_http_provider_response_limit_status_redirect_and_timeout():
    responses = [
        httpx.Response(200, content="x" * 100),
        httpx.Response(401),
        httpx.Response(302, headers={"Location": "https://another.invalid/steal"}),
    ]
    for response in responses:

        async def handler(request, response=response):
            return response

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            provider = HTTPEmbeddingProvider(
                "https://embedding.invalid/embeddings", "model", max_response_bytes=10, client=client
            )
            with pytest.raises(EmbeddingError):
                await provider.embed(["query"])

    async def timeout(request):
        raise httpx.ReadTimeout("timed out")

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        with pytest.raises(EmbeddingError, match="request failed"):
            await HTTPEmbeddingProvider("https://embedding.invalid/embeddings", "model", client=client).embed(
                ["q"]
            )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"endpoint": "file:///etc/passwd"},
        {"endpoint": "https://user:secret@example.com/v1/embeddings"},
        {"model": ""},
        {"api_key": "secret\r\nInjection: value"},
        {"timeout": 0},
        {"timeout": float("inf")},
        {"batch_size": 0},
        {"batch_size": True},
        {"max_response_bytes": 0},
    ],
)
def test_http_provider_configuration_validation(kwargs):
    options = {"endpoint": "https://embedding.invalid/embeddings", "model": "model", **kwargs}
    with pytest.raises(ValueError):
        HTTPEmbeddingProvider(**options)


@pytest.mark.asyncio
async def test_synthetic_3000_tool_benchmark():
    """Reproducible scaling smoke measurement, NOT a semantic/recall benchmark.

    Reproduce with pytest -s tests/unit/test_retrieval.py -k synthetic_3000.
    No timing threshold is asserted because CI hardware is not controlled.
    """
    import os
    import platform
    import statistics
    import time

    definitions = [
        tool(
            f"compute__inspectResource{index:04}",
            f"Inspect resource inventory status in region {index % 50}",
            {"resourceId": {"type": "string"}, "regionId": {"type": "string"}},
        )
        for index in range(3000)
    ]
    allowed = {item["name"] for index, item in enumerate(definitions) if index % 2 == 0}
    started = time.perf_counter()
    retriever = HybridRetriever(definitions)
    await retriever.build()
    build_ms = (time.perf_counter() - started) * 1000
    timings_ms = []
    for region in range(10):
        started = time.perf_counter()
        result = await retriever.search(f"Inspect resource inventory region {region} resourceId", allowed)
        timings_ms.append((time.perf_counter() - started) * 1000)
        assert len(result) == 15
        assert all(item["tool"]["name"] in allowed for item in result)
    report = {
        "benchmark": "synthetic_3000_tools_no_accuracy_claim",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "visible_cpu_count": os.cpu_count(),
        "tool_count": len(definitions),
        "authorized_tool_count": len(allowed),
        "embedding": "deterministic_lexical_hash_NOT_pretrained_semantic",
        "dimensions": 384,
        "dense_search": "exact_cosine_NOT_ANN",
        "queries": len(timings_ms),
        "index_build_ms": round(build_ms, 3),
        "search_median_ms": round(statistics.median(timings_ms), 3),
        "search_max_ms": round(max(timings_ms), 3),
        "search_timings_ms": [round(t, 3) for t in timings_ms],
        "authorized_only": True,
        "result_count_per_query": 15,
    }
    print("\nSYNTHETIC_RETRIEVAL_BENCHMARK=" + json.dumps(report, sort_keys=True))

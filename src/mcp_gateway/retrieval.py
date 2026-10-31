"""ACL-scoped BM25 + cosine retrieval fused using the paper's RRF parameters.

The default dense representation is deterministic *lexical feature hashing*, NOT
semantic/pretrained embeddings. Configure ``HTTPEmbeddingProvider`` to use an
actual embedding model. Cosine search here is exact, rather than approximate
nearest-neighbour search; this deliberately favours auditability at small scale.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections import Counter
from copy import deepcopy
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

CANDIDATE_LIMIT = 120
RRF_K = 20
BM25_K1 = 1.2
BM25_B = 0.75
MAX_QUERY_LENGTH = 8192
_STOP_WORDS = frozenset(
    "a an and are as at be been being by for from has have in into is it its "
    "of on or that the their these this those to was were will with".split()
)


class EmbeddingError(ValueError):
    """Embedding service or vector validation failed (never silently downgraded)."""


class EmbeddingProvider(Protocol):
    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one nonzero, finite, same-dimensional vector per input text."""
        ...


def tokenize(text: str) -> list[str]:
    """Split identifiers, preserve Unicode words, lowercase and remove stopwords."""
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    return [
        word for word in re.findall(r"[^\W_]+", text.lower(), flags=re.UNICODE) if word not in _STOP_WORDS
    ]


def tool_text(tool: dict[str, Any]) -> str:
    """Only name, description and top-level parameter names enter the index."""
    schema = tool.get("inputSchema", {})
    properties = schema.get("properties", {})
    return " ".join((tool["name"], tool.get("description", ""), *properties))


class DeterministicEmbedding:
    """Offline lexical hash vectors; not semantic and not a pretrained encoder.

    Signed token and character-trigram features provide a stable smoke-test and
    development mode without model downloads, network calls, or credentials.
    """

    semantic = False

    def __init__(self, dimensions: int = 384):
        if isinstance(dimensions, bool) or not isinstance(dimensions, int) or dimensions < 16:
            raise ValueError("dimensions must be an integer >= 16")
        self.dimensions = dimensions

    async def embed(self, texts: list[str]) -> list[list[float]]:
        results = []
        for text in texts:
            vector = [0.0] * self.dimensions
            terms = tokenize(text) or ["<empty>"]
            for term in terms:
                features = [("word:" + term, 2.0)]
                padded = "^" + term + "$"
                features.extend(("tri:" + padded[i : i + 3], 0.25) for i in range(max(0, len(padded) - 2)))
                for feature, weight in features:
                    digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
                    bucket = int.from_bytes(digest[:7], "big") % self.dimensions
                    vector[bucket] += weight if digest[7] & 1 else -weight
            # Rare total hash cancellation must not produce an invalid vector.
            if not any(vector):
                vector[0] = 1.0
            results.append(vector)
        return results


class HTTPEmbeddingProvider:
    """Opt-in OpenAI-compatible ``POST /embeddings`` JSON adapter.

    Supply the full endpoint and model name. Authentication is backend-specific;
    caller ingress credentials must never be used as ``api_key``. Redirects and
    environment-derived proxies are disabled for the owned HTTP client. Requests
    are batched and bounded; failed/invalid responses raise ``EmbeddingError``.
    An injected client is caller-owned, primarily useful for tests.
    """

    semantic = True

    def __init__(
        self,
        endpoint: str,
        model: str,
        api_key: str | None = None,
        *,
        timeout: float = 15.0,
        batch_size: int = 128,
        max_response_bytes: int = 16 * 1024 * 1024,
        client: httpx.AsyncClient | None = None,
    ):
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise ValueError("embedding endpoint must be an absolute HTTP(S) URL without userinfo/fragment")
        if not isinstance(model, str) or not model.strip():
            raise ValueError("embedding model is required")
        if api_key is not None and (not isinstance(api_key, str) or "\r" in api_key or "\n" in api_key):
            raise ValueError("invalid embedding API key")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("embedding timeout must be finite and positive")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("embedding batch size must be positive")
        if (
            isinstance(max_response_bytes, bool)
            or not isinstance(max_response_bytes, int)
            or max_response_bytes < 1
        ):
            raise ValueError("embedding response limit must be positive")
        self.endpoint, self.model = endpoint, model
        self._api_key = api_key
        self.timeout, self.batch_size = timeout, batch_size
        self.max_response_bytes = max_response_bytes
        self._client = client

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if any(not isinstance(text, str) for text in texts):
            raise ValueError("embedding inputs must be strings")
        if self._client is not None:
            return await self._embed_with_client(self._client, texts)
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=False, trust_env=False) as client:
            return await self._embed_with_client(client, texts)

    async def _embed_with_client(self, client: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
        vectors = []
        headers = {"Authorization": "Bearer " + self._api_key} if self._api_key else {}
        for offset in range(0, len(texts), self.batch_size):
            batch = texts[offset : offset + self.batch_size]
            try:
                async with client.stream(
                    "POST",
                    self.endpoint,
                    headers=headers,
                    json={"model": self.model, "input": batch, "encoding_format": "float"},
                    timeout=self.timeout,
                    follow_redirects=False,
                ) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > self.max_response_bytes:
                            raise EmbeddingError("embedding response exceeds configured byte limit")
                payload = json.loads(body)
            except (httpx.HTTPError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise EmbeddingError("embedding service request failed") from exc
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise EmbeddingError("embedding response has no data list")
            data = payload["data"]
            if len(data) != len(batch):
                raise EmbeddingError("embedding response count does not match input count")
            ordered: dict[int, list[float]] = {}
            for item in data:
                if not isinstance(item, dict):
                    raise EmbeddingError("embedding response item must be an object")
                index = item.get("index")
                if (
                    isinstance(index, bool)
                    or not isinstance(index, int)
                    or not 0 <= index < len(batch)
                    or index in ordered
                ):
                    raise EmbeddingError("embedding response indices are invalid")
                ordered[index] = item.get("embedding")
            vectors.extend(ordered[index] for index in range(len(batch)))
        # Validate before returning, without forcing callers to know the adapter.
        _normalize_vectors(vectors, expected_count=len(texts))
        return vectors


def _normalize_vectors(
    vectors: Any,
    *,
    expected_count: int,
    dimensions: int | None = None,
) -> list[tuple[float, ...]]:
    if not isinstance(vectors, (list, tuple)) or len(vectors) != expected_count:
        raise EmbeddingError("embedding vector count does not match input count")
    normalized = []
    for vector in vectors:
        if not isinstance(vector, (list, tuple)) or not vector:
            raise EmbeddingError("embedding vectors must be nonempty numeric arrays")
        if dimensions is None:
            dimensions = len(vector)
        if len(vector) != dimensions:
            raise EmbeddingError("embedding vector dimensions are inconsistent")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in vector):
            raise EmbeddingError("embedding vectors contain nonnumeric values")
        try:
            values = tuple(float(value) for value in vector)
        except (ValueError, OverflowError) as exc:
            raise EmbeddingError("embedding vector values are out of range") from exc
        if not all(math.isfinite(value) for value in values):
            raise EmbeddingError("embedding vectors must contain finite values")
        # hypot avoids overflow when the components themselves are finite.
        norm = math.hypot(*values)
        if norm == 0 or not math.isfinite(norm):
            raise EmbeddingError("embedding vectors must have a finite, nonzero norm")
        normalized.append(tuple(value / norm for value in values))
    return normalized


def reciprocal_rank_fusion(
    rankings: list[list[str]],
    *,
    k: int = RRF_K,
) -> list[tuple[str, float]]:
    """One-based ranks, outer union, deterministic name ordering for ties."""
    if isinstance(k, bool) or not isinstance(k, int) or k < 0:
        raise ValueError("RRF smoothing constant must be a nonnegative integer")
    scores: dict[str, float] = {}
    for ranking in rankings:
        seen = set()
        for rank, name in enumerate(ranking, start=1):
            if name in seen:
                raise ValueError("a ranking must not contain duplicate names")
            seen.add(name)
            scores[name] = scores.get(name, 0.0) + 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))


class HybridRetriever:
    """Immutable mounted-tool index with ACL filtering before both rankings.

    BM25 corpus statistics are recomputed from only authorized documents, so an
    inaccessible tool cannot change an authorized result's lexical score. No
    cross-user query/result cache is kept. Definitions are copied on input and
    output; mutating a returned schema cannot modify another caller's result.
    """

    def __init__(self, tools: list[dict], embedding_provider: EmbeddingProvider | None = None):
        self._tools: dict[str, dict] = {}
        for tool in tools:
            if not isinstance(tool, dict) or not isinstance(tool.get("name"), str) or not tool["name"]:
                raise ValueError("each tool requires a nonempty string name")
            name = tool["name"]
            if name in self._tools:
                raise ValueError("duplicate tool name: " + name)
            if not isinstance(tool.get("description", ""), str):
                raise ValueError("tool description must be a string")
            schema = tool.get("inputSchema", {})
            if (
                not isinstance(schema, dict)
                or not isinstance(schema.get("properties", {}), dict)
                or any(not isinstance(key, str) for key in schema.get("properties", {}))
            ):
                raise ValueError("tool inputSchema properties must be a string-keyed object")
            self._tools[name] = deepcopy(tool)
        self._names = sorted(self._tools)
        self._texts = {name: tool_text(self._tools[name]) for name in self._names}
        self._tokens = {name: Counter(tokenize(self._texts[name])) for name in self._names}
        self._provider = embedding_provider or DeterministicEmbedding()
        self._vectors: dict[str, tuple[float, ...]] | None = None
        self._build_lock = asyncio.Lock()

    async def build(self) -> None:
        """Build dense vectors once; invalid providers fail atomically."""
        async with self._build_lock:
            if self._vectors is not None:
                return
            if not self._names:
                self._vectors = {}
                return
            vectors = await self._provider.embed([self._texts[name] for name in self._names])
            normalized = _normalize_vectors(vectors, expected_count=len(self._names))
            self._vectors = dict(zip(self._names, normalized, strict=True))

    def _sparse_ranking(self, query_tokens: list[str], names: list[str]) -> list[str]:
        count = len(names)
        lengths = {name: sum(self._tokens[name].values()) for name in names}
        average_length = sum(lengths.values()) / count if count else 0.0
        if not average_length:
            return []
        query_terms = sorted(set(query_tokens))
        document_frequencies = {
            term: sum(term in self._tokens[name] for name in names) for term in query_terms
        }
        scores = {}
        for name in names:
            score = 0.0
            for term in query_terms:
                frequency = self._tokens[name][term]
                if not frequency:
                    continue
                document_frequency = document_frequencies[term]
                inverse_document_frequency = math.log(
                    1.0 + (count - document_frequency + 0.5) / (document_frequency + 0.5)
                )
                denominator = frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * lengths[name] / average_length)
                score += inverse_document_frequency * frequency * (BM25_K1 + 1.0) / denominator
            if score > 0:
                scores[name] = score
        return sorted(scores, key=lambda name: (-scores[name], name))[:CANDIDATE_LIMIT]

    async def search(self, query: str, allowed_names: set[str], top_k: int = 15) -> list[dict]:
        if not isinstance(query, str) or len(query) > MAX_QUERY_LENGTH:
            raise ValueError(f"query must be a string of at most {MAX_QUERY_LENGTH} characters")
        if isinstance(top_k, bool) or not isinstance(top_k, int) or not 1 <= top_k <= CANDIDATE_LIMIT:
            raise ValueError(f"top_k must be an integer between 1 and {CANDIDATE_LIMIT}")
        if not isinstance(allowed_names, (set, frozenset)) or any(
            not isinstance(name, str) for name in allowed_names
        ):
            raise ValueError("allowed_names must be a set of tool names")
        names = [name for name in self._names if name in allowed_names]
        query_tokens = tokenize(query)
        if not names or not query_tokens:
            return []
        await self.build()
        assert self._vectors is not None
        dimensions = len(self._vectors[names[0]])
        raw_query_vector = await self._provider.embed([query])
        query_vector = _normalize_vectors(raw_query_vector, expected_count=1, dimensions=dimensions)[0]
        dense_scores = {
            name: math.fsum(a * b for a, b in zip(query_vector, self._vectors[name], strict=True))
            for name in names
        }
        dense = sorted(names, key=lambda name: (-dense_scores[name], name))[:CANDIDATE_LIMIT]
        sparse = self._sparse_ranking(query_tokens, names)
        ranked = reciprocal_rank_fusion([dense, sparse])[:top_k]
        return [{"tool": deepcopy(self._tools[name]), "score": score} for name, score in ranked]

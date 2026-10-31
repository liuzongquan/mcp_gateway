"""Unit and HTTP-serialization contract tests for the fail-fast compiler."""

import copy
import json

import httpx
import pytest

from mcp_gateway.openapi import OpenAPICompileError, compile_openapi


def spec(operation=None, *, path="/pets", version="3.1.0"):
    return {
        "openapi": version,
        "info": {"title": "Pet API", "version": "1"},
        "servers": [{"url": "https://api.example.test/v1"}],
        "paths": {path: {"get": operation or {"operationId": "listPets"}}},
    }


def parameter(name="limit", location="query", kind="integer", **extra):
    return {"name": name, "in": location, "schema": {"type": kind}, **extra}


def compile_one(document):
    return compile_openapi(document, "pets")[0]


def test_metadata_and_deterministic_names_are_isolated():
    document = spec({"operationId": "get-pets", "summary": "List pets", "description": "Find active pets."})
    original = copy.deepcopy(document)
    operation = compile_one(document)
    assert operation.name == "pets__get_pets"
    assert operation.description == "List pets\n\nFind active pets."
    assert operation.method == "GET"
    assert operation.tool_definition() == {
        "name": "pets__get_pets",
        "description": operation.description,
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    }
    operation.tool_definition()["inputSchema"]["properties"]["leak"] = {}
    assert "leak" not in operation.input_schema["properties"]
    assert document == original
    assert compile_one(spec({"summary": "No operation ID"})).name == "pets__get__pets"


def test_parameter_and_body_mapping_preserves_locations_and_encoding():
    document = spec(
        {
            "operationId": "updatePet",
            "parameters": [
                parameter("petId", "path", "string", required=True),
                parameter("tag", kind="array", schema={"type": "array", "items": {"type": "string"}}),
                parameter("active", kind="boolean"),
                parameter("X-Region", "header", "string"),
            ],
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                            "additionalProperties": False,
                        }
                    }
                },
            },
        },
        path="/pets/{petId}",
    )
    operation = compile_one(document)
    method, url, kwargs = operation.build_request(
        {
            "petId": "a/b?中",
            "tag": ["a b", "two"],
            "active": False,
            "X-Region": "west",
            "body": {"name": "Pip"},
        }
    )
    request = httpx.Request(method, url, **kwargs)
    assert request.url.raw_path == b"/v1/pets/a%2Fb%3F%E4%B8%AD?tag=a+b&tag=two&active=false"
    assert request.headers["X-Region"] == "west"
    assert json.loads(request.content) == {"name": "Pip"}
    assert request.headers["content-type"] == "application/json"


def test_required_type_constraints_extra_args_and_body_validation():
    operation = compile_one(
        spec(
            {
                "parameters": [
                    parameter(required=True, schema={"type": "integer", "minimum": 1, "maximum": 10})
                ],
                "requestBody": {
                    "required": True,
                    "content": {"application/json": {"schema": {"type": "string"}}},
                },
            }
        )
    )
    for arguments in (
        {},
        {"limit": 2},
        {"limit": "2", "body": "x"},
        {"limit": 0, "body": "x"},
        {"limit": 2, "body": {}, "extra": "x"},
        {"limit": 2, "body": "x", "extra": "x"},
        [],
    ):
        with pytest.raises(ValueError, match="Invalid"):
            operation.build_request(arguments)
    assert operation.build_request({"limit": 2, "body": "x"})[2]["json"] == "x"


def test_optional_body_omitted_and_null_encoded_as_json_null():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": ["object", "null"],
                            }
                        }
                    }
                }
            }
        )
    )
    assert "json" not in operation.build_request({})[2]
    method, url, kwargs = operation.build_request({"body": None})
    request = httpx.Request(method, url, **kwargs)
    assert request.content == b"null"
    assert request.headers["content-type"] == "application/json"


def test_local_refs_and_escaped_json_pointers():
    document = spec(
        {
            "parameters": [{"$ref": "#/components/parameters/limit"}],
            "requestBody": {
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/a~1b~0c"}}}
            },
        }
    )
    document["components"] = {
        "parameters": {"limit": parameter()},
        "schemas": {"a/b~c": {"type": "object", "properties": {"title": {"type": "string"}}}},
    }
    operation = compile_one(document)
    assert operation.input_schema["properties"]["limit"]["type"] == "integer"
    assert operation.body_schema["properties"]["title"]["type"] == "string"


@pytest.mark.parametrize(
    "reference, expected",
    [
        ("https://evil.test/spec.json", "Only local"),
        ("#/absent", "Unresolved"),
        ("#/components/schemas/loop", "Cyclic"),
    ],
)
def test_invalid_refs_fail_at_mount(reference, expected):
    document = spec({"requestBody": {"content": {"application/json": {"schema": {"$ref": reference}}}}})
    document["components"] = {
        "schemas": {"loop": {"type": "object", "properties": {"next": {"$ref": "#/components/schemas/loop"}}}}
    }
    with pytest.raises(OpenAPICompileError, match=expected):
        compile_one(document)


def test_path_parameter_override_and_simple_array_style():
    document = spec(
        {
            "parameters": [
                parameter(
                    "ids",
                    "path",
                    "array",
                    required=True,
                    schema={"type": "array", "items": {"type": "integer"}, "minItems": 1},
                )
            ]
        },
        path="/pets/{ids}",
    )
    document["paths"]["/pets/{ids}"]["parameters"] = [parameter("ids", "path", "string", required=True)]
    operation = compile_one(document)
    assert len(operation.parameters) == 1
    assert operation.build_request({"ids": [2, 3]})[1].endswith("/pets/2,3")


def test_query_array_non_exploded_and_header_array():
    operation = compile_one(
        spec(
            {
                "parameters": [
                    parameter(
                        "ids",
                        kind="array",
                        explode=False,
                        schema={"type": "array", "items": {"type": "integer"}},
                    ),
                    parameter(
                        "X-IDs", "header", "array", schema={"type": "array", "items": {"type": "integer"}}
                    ),
                ]
            }
        )
    )
    kwargs = operation.build_request({"ids": [1, 2], "X-IDs": [3, 4]})[2]
    assert kwargs["params"] == [("ids", "1,2")]
    assert kwargs["headers"] == {"X-IDs": "3,4"}


@pytest.mark.parametrize(
    "name",
    [
        "Authorization",
        "COOKIE",
        "Host",
        "Content-Length",
        "Transfer-Encoding",
        "Forwarded",
        "X-Forwarded-Host",
        "Proxy-Authorization",
        "X-API-Key",
        "Mcp-Session-Id",
        "Mcp-Protocol-Version",
        "Content-Type",
    ],
)
def test_transport_and_credential_headers_cannot_be_tool_arguments(name):
    with pytest.raises(OpenAPICompileError, match="Sensitive/transport"):
        compile_one(spec({"parameters": [parameter(name, "header", "string")]}))


def test_header_newline_injection_is_rejected_before_http_call():
    operation = compile_one(spec({"parameters": [parameter("X-Region", "header", "string")]}))
    with pytest.raises(ValueError, match="CR/LF"):
        operation.build_request({"X-Region": "west\r\nAuthorization: injected"})


def test_path_dot_segment_cannot_change_routing():
    operation = compile_one(
        spec({"parameters": [parameter("id", "path", "string", required=True)]}, path="/pets/{id}")
    )
    for value in (".", ".."):
        method, url, kwargs = operation.build_request({"id": value})
        request = httpx.Request(method, url, **kwargs)
        assert request.url.raw_path == b"/v1/pets/" + b"%2E" * len(value)


@pytest.mark.parametrize(
    "bad_param",
    [
        parameter("id", "cookie", "string"),
        parameter(kind="object"),
        parameter(kind="array", schema={"type": "array", "items": {"type": "object"}}),
        parameter(style="deepObject"),
        parameter(allowReserved=True),
        parameter(allowEmptyValue=True),
        parameter(explode="true"),
        parameter(schema={"type": ["integer", "null"]}),
        parameter(schema={"type": "integer", "minimum": "no"}),
        {"name": "q", "in": "query", "content": {"application/json": {}}},
        parameter("bad\r\nname", "header", "string"),
    ],
)
def test_unsupported_or_invalid_parameters_fail_at_mount(bad_param):
    with pytest.raises(OpenAPICompileError):
        compile_one(spec({"parameters": [bad_param]}))


@pytest.mark.parametrize(
    "document",
    [
        spec({"parameters": [parameter("id", "path", "string")]}, path="/pets/{id}"),
        spec({"parameters": []}, path="/pets/{id}"),
        spec({"parameters": [parameter("id", "path", "string", required=True)]}),
        spec({"parameters": [parameter(), parameter()]}),
        spec({"parameters": [parameter("limit"), parameter("limit", "header", "string")]}),
        spec(
            {
                "parameters": [parameter("body")],
                "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
            }
        ),
        spec({"requestBody": {"content": {"multipart/form-data": {"schema": {"type": "object"}}}}}),
        spec({"callbacks": {"event": {}}}),
    ],
)
def test_ambiguous_and_unsupported_specifications_fail_at_mount(document):
    with pytest.raises(OpenAPICompileError):
        compile_one(document)


def test_duplicate_normalized_operation_names_fail_at_mount():
    document = spec({"operationId": "list-pets"})
    document["paths"]["/other"] = {"get": {"operationId": "list_pets"}}
    with pytest.raises(OpenAPICompileError, match="Duplicate normalized"):
        compile_one(document)


def test_openapi_30_nullable_and_exclusive_bounds_are_converted():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "number",
                                "nullable": True,
                                "minimum": 0,
                                "exclusiveMinimum": True,
                                "maximum": 10,
                                "exclusiveMaximum": False,
                            }
                        }
                    }
                }
            },
            version="3.0.3",
        )
    )
    assert operation.body_schema == {"type": ["number", "null"], "exclusiveMinimum": 0, "maximum": 10}
    operation.build_request({"body": None})
    operation.build_request({"body": 10})
    with pytest.raises(ValueError):
        operation.build_request({"body": 0})


def test_openapi_30_additional_properties_boolean_supported():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                            }
                        }
                    }
                }
            },
            version="3.0.3",
        )
    )
    operation.build_request({"body": {}})
    with pytest.raises(ValueError):
        operation.build_request({"body": {"extra": 1}})


def test_schema_composition_and_format_are_enforced():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "oneOf": [
                                    {"type": "string", "format": "date"},
                                    {"type": "integer", "minimum": 1},
                                ],
                            }
                        }
                    }
                }
            }
        )
    )
    operation.build_request({"body": "2026-10-31"})
    operation.build_request({"body": 3})
    with pytest.raises(ValueError):
        operation.build_request({"body": "not-a-date"})


def test_server_precedence_variables_and_explicit_override():
    document = spec(
        {
            "servers": [
                {
                    "url": "https://{region}.example.test/{version}",
                    "variables": {
                        "region": {"default": "eu"},
                        "version": {"default": "v2"},
                    },
                }
            ]
        }
    )
    document["paths"]["/pets"]["servers"] = [{"url": "https://path.example.test"}]
    assert compile_one(document).base_url == "https://eu.example.test/v2"
    assert (
        compile_openapi(document, "pets", base_url="http://localhost:8765")[0].base_url
        == "http://localhost:8765"
    )
    del document["paths"]["/pets"]["get"]["servers"]
    assert compile_one(document).base_url == "https://path.example.test"


@pytest.mark.parametrize(
    "url",
    [
        "/relative",
        "ftp://example.test",
        "https://user:secret@example.test",
        "https://example.test?q=x",
        "https://example.test#frag",
        "https://{unresolved}.test",
        "http://bad port",
        "http://example.test:bad",
    ],
)
def test_invalid_server_urls_fail_at_mount(url):
    document = spec()
    document["servers"] = [{"url": url}]
    with pytest.raises(OpenAPICompileError):
        compile_one(document)


@pytest.mark.parametrize(
    "path",
    [
        "relative",
        "//evil.test",
        "/../admin",
        "/pets?query",
        "/pets#frag",
        "/\\evil",
        "/pets/{id",
        "/bad path",
    ],
)
def test_invalid_paths_fail_at_mount(path):
    with pytest.raises(OpenAPICompileError):
        compile_one(spec(path=path))


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"openapi": "2.0", "paths": {}},
        {"openapi": "3.1.0", "paths": []},
        {"openapi": "3.1.0", "paths": {}},
    ],
)
def test_missing_or_unsupported_documents_fail_at_mount(document):
    with pytest.raises(OpenAPICompileError):
        compile_one(document)


def test_security_declarations_do_not_become_model_visible_credentials():
    document = spec({"security": [{"apiKey": []}]})
    document["components"] = {
        "securitySchemes": {"apiKey": {"type": "apiKey", "in": "header", "name": "X-Secret"}}
    }
    operation = compile_one(document)
    assert operation.input_schema["properties"] == {}
    assert operation.build_request({})[2]["headers"] == {}


@pytest.mark.parametrize(
    "value", ["a/b", "%2F", "../x", "?query=secret", "#fragment", "https://evil.test/x", "//evil.test"]
)
def test_adversarial_path_value_stays_encoded_and_same_origin(value):
    operation = compile_one(
        spec({"parameters": [parameter("id", "path", "string", required=True)]}, path="/pets/{id}")
    )
    method, url, kwargs = operation.build_request({"id": value})
    request = httpx.Request(method, url, **kwargs)
    assert request.url.host == "api.example.test"
    assert request.url.query == b""
    assert request.url.fragment == ""
    raw_suffix = request.url.raw_path.removeprefix(b"/v1/pets/")
    assert b"/" not in raw_suffix
    assert b"?" not in raw_suffix
    assert b"#" not in raw_suffix
    if "%" in value:
        assert b"%25" in raw_suffix


@pytest.mark.parametrize("location, name", [("header", "X-Secret"), ("query", "secretKey")])
def test_custom_security_scheme_credentials_cannot_be_model_inputs(location, name):
    document = spec({"parameters": [parameter(name, location, "string")]})
    document["components"] = {
        "securitySchemes": {"customKey": {"type": "apiKey", "in": location, "name": name}}
    }
    with pytest.raises(OpenAPICompileError, match="egress authentication"):
        compile_one(document)


def test_schema_literals_and_property_names_are_not_treated_as_refs():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"$ref": {"type": "string"}},
                                "default": {"$ref": "a literal data value, not a JSON reference"},
                                "const": {"$ref": "data"},
                            }
                        }
                    }
                }
            }
        )
    )
    operation.build_request({"body": {"$ref": "data"}})


def test_uri_encoded_json_pointer_references_resolve():
    document = spec(
        {"requestBody": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/a%20b"}}}}}
    )
    document["components"] = {"schemas": {"a b": {"type": "integer"}}}
    assert compile_one(document).body_schema == {"type": "integer"}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), object()])
def test_non_json_or_non_finite_values_are_rejected(value):
    operation = compile_one(
        spec({"requestBody": {"content": {"application/json": {"schema": {"type": "number"}}}}})
    )
    with pytest.raises(ValueError, match="finite JSON"):
        operation.build_request({"body": value})


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "string", "minLenght": 2},
        {"type": "array", "additionalItems": False},
        {"type": "object", "dependencies": {"x": ["y"]}},
    ],
)
def test_unknown_or_obsolete_schema_constraints_are_not_silently_ignored(schema):
    with pytest.raises(OpenAPICompileError, match="Unsupported JSON Schema keyword"):
        compile_one(spec({"requestBody": {"content": {"application/json": {"schema": schema}}}}))


def test_read_only_required_fields_are_response_only():
    operation = compile_one(
        spec(
            {
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer", "readOnly": True},
                                    "name": {"type": "string"},
                                },
                                "required": ["id", "name"],
                            }
                        }
                    }
                }
            }
        )
    )
    operation.build_request({"body": {"name": "Pip"}})
    assert operation.body_schema["required"] == ["name"]
    assert operation.body_schema["properties"]["id"]["readOnly"] is True


def test_non_string_path_is_a_compile_error_not_an_attribute_error():
    document = spec()
    document["paths"] = {17: {"get": {}}}
    with pytest.raises(OpenAPICompileError):
        compile_one(document)


@pytest.mark.parametrize("value", ["west\x00evil", "west\x7fevil", "地域"])
def test_unrepresentable_header_values_are_rejected(value):
    operation = compile_one(spec({"parameters": [parameter("X-Region", "header", "string")]}))
    with pytest.raises(ValueError, match="printable ASCII"):
        operation.build_request({"X-Region": value})


@pytest.mark.parametrize(
    "url", ["http://example.test\\evil", "http://example.test\x00evil", "http://example.test\x7fevil"]
)
def test_control_characters_and_backslashes_are_rejected_in_base_urls(url):
    with pytest.raises(OpenAPICompileError):
        compile_openapi(spec(), "pets", base_url=url)


@pytest.mark.parametrize("variables", [[], {1: {"default": "v1"}}, {"region": {}}])
def test_malformed_server_variables_are_compile_errors(variables):
    document = spec()
    document["servers"][0]["variables"] = variables
    with pytest.raises(OpenAPICompileError):
        compile_one(document)


def test_invalid_parameter_description_is_compile_error():
    with pytest.raises(OpenAPICompileError, match="description"):
        compile_one(spec({"parameters": [parameter(description=42)]}))

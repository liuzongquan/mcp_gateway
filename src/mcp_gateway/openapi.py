"""Fail-fast OpenAPI 3.0/3.1 compilation into deterministic MCP tools.

The supported surface is deliberately explicit: local acyclic references,
path/query/header primitive or primitive-array parameters, and JSON bodies.
Unsupported wire formats are rejected at mount time, never approximated at call
time. Backend credentials belong to gateway configuration, not tool arguments.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, unquote, urlsplit

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError

_HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
_SECRET_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "upgrade",
    "trailer",
    "te",
    "forwarded",
    "x-api-key",
    "api-key",
    "x-auth-token",
    "x-access-token",
    "x-real-ip",
    "mcp-session-id",
    "mcp-protocol-version",
    "content-type",
}
_METADATA = {"example", "xml", "externalDocs", "deprecated"}
_SCHEMA_MAPS = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
_SCHEMA_ARRAYS = {"allOf", "anyOf", "oneOf", "prefixItems"}
_SCHEMA_SINGLE = {
    "items",
    "additionalProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
    "contains",
    "not",
    "if",
    "then",
    "else",
    "propertyNames",
}
_PRIMITIVES = {"string", "integer", "number", "boolean"}
_SCHEMA_KEYWORDS = set(Draft202012Validator.VALIDATORS) | {
    "$schema",
    "$id",
    "$anchor",
    "$dynamicAnchor",
    "$comment",
    "$defs",
    "definitions",
    "title",
    "description",
    "default",
    "examples",
    "readOnly",
    "writeOnly",
    "then",
    "else",
    "minContains",
    "maxContains",
    "contentEncoding",
    "contentMediaType",
    "contentSchema",
}


class OpenAPICompileError(ValueError):
    """The source specification cannot be represented without losing semantics."""


@dataclass(frozen=True)
class ParameterSpec:
    name: str
    location: str
    schema: dict[str, Any]
    required: bool
    style: str
    explode: bool


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


@dataclass(frozen=True)
class CompiledOperation:
    name: str
    description: str
    input_schema: dict[str, Any]
    method: str
    base_url: str
    path: str
    parameters: tuple[ParameterSpec, ...]
    body_required: bool = False
    body_schema: dict[str, Any] | bool | None = None

    def tool_definition(self) -> dict[str, Any]:
        """Return a fresh MCP Tool representation, safe for callers to mutate."""
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": copy.deepcopy(self.input_schema),
        }

    def build_request(self, arguments: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
        """Validate once, then execute the compiled HTTP parameter mapping.

        JSON bodies use the argument named ``body``; all ordinary parameters
        retain their OpenAPI names. Return values can be passed to
        ``httpx.AsyncClient.request(method, url, **kwargs)``.
        """
        try:
            json.dumps(arguments, allow_nan=False)
        except (TypeError, ValueError):
            raise ValueError("Arguments must contain finite JSON-compatible values") from None
        try:
            Draft202012Validator(self.input_schema, format_checker=FormatChecker()).validate(arguments)
        except ValidationError as exc:
            path = ".".join(str(part) for part in exc.absolute_path) or "arguments"
            raise ValueError(f"Invalid {path}: {exc.message}") from None
        path = self.path
        query: list[tuple[str, str]] = []
        headers: dict[str, str] = {}
        for param in self.parameters:
            if param.name not in arguments:
                continue
            value = arguments[param.name]
            values = [_scalar(item) for item in value] if isinstance(value, list) else [_scalar(value)]
            if param.location == "path":
                # Encode array items individually: literal commas in an item are
                # data, but inserted commas are the simple-style delimiter.
                encoded = ",".join(quote(item, safe="") for item in values)
                if encoded in {".", ".."}:
                    encoded = encoded.replace(".", "%2E")
                path = path.replace("{" + param.name + "}", encoded)
            elif param.location == "query":
                if isinstance(value, list) and param.explode:
                    query.extend((param.name, item) for item in values)
                else:
                    query.append((param.name, ",".join(values)))
            else:
                serialized = ",".join(values)
                if "\r" in serialized or "\n" in serialized:
                    raise ValueError(f"Invalid header {param.name}: CR/LF is forbidden")
                if any(ord(ch) < 32 or ord(ch) >= 127 for ch in serialized):
                    raise ValueError(f"Invalid header {param.name}: only printable ASCII is supported")
                headers[param.name] = serialized
        kwargs: dict[str, Any] = {"params": query, "headers": headers}
        if self.body_schema is not None and "body" in arguments:
            # httpx treats json=None as no body, so serialize null explicitly.
            if arguments["body"] is None:
                kwargs["content"] = b"null"
                headers["Content-Type"] = "application/json"
            else:
                kwargs["json"] = arguments["body"]
        return self.method, self.base_url.rstrip("/") + path, kwargs


class _Compiler:
    def __init__(self, document: dict[str, Any], version: str):
        self.document = document
        self.is_30 = version.startswith("3.0.")

    def resolve(self, value: Any, trail: tuple[str, ...] = ()) -> Any:
        if isinstance(value, list):
            return [self.resolve(item, trail) for item in value]
        if not isinstance(value, dict):
            return copy.deepcopy(value)
        if "$dynamicRef" in value or "$recursiveRef" in value:
            raise OpenAPICompileError("Dynamic/recursive schema references are unsupported")
        if "$ref" in value:
            ref = value["$ref"]
            if not isinstance(ref, str) or not ref.startswith("#/"):
                raise OpenAPICompileError(f"Only local JSON-pointer $refs are supported: {ref!r}")
            if ref in trail:
                raise OpenAPICompileError(f"Cyclic $ref is unsupported: {' -> '.join((*trail, ref))}")
            target: Any = self.document
            try:
                for part in unquote(ref[2:]).split("/"):
                    key = part.replace("~1", "/").replace("~0", "~")
                    target = target[int(key)] if isinstance(target, list) else target[key]
            except (KeyError, IndexError, TypeError, ValueError):
                raise OpenAPICompileError(f"Unresolved $ref: {ref}") from None
            resolved = self.resolve(target, (*trail, ref))
            siblings = {key: item for key, item in value.items() if key != "$ref"}
            if siblings:
                # Avoid conflating OpenAPI Reference Objects with 3.1 Schema
                # Objects: callers must use allOf for schema composition.
                if not isinstance(resolved, dict) or any(
                    key not in {"description", "summary"} for key in siblings
                ):
                    raise OpenAPICompileError(
                        "$ref siblings other than description/summary are unsupported; use allOf"
                    )
                resolved.update(self.resolve(siblings, trail))
            return resolved
        resolved = {}
        for key, item in value.items():
            if key in {"default", "const", "enum", "example", "examples"}:
                # These contain arbitrary JSON data, not schema/reference objects.
                resolved[key] = copy.deepcopy(item)
            elif key in _SCHEMA_MAPS and isinstance(item, dict):
                # Property/definition names may themselves be "$ref".
                resolved[key] = {name: self.resolve(child, trail) for name, child in item.items()}
            else:
                resolved[key] = self.resolve(item, trail)
        return resolved

    def schema(self, schema: Any) -> dict[str, Any] | bool:
        if isinstance(schema, bool):
            if self.is_30:
                raise OpenAPICompileError("Boolean schemas require OpenAPI 3.1")
            return schema
        if not isinstance(schema, dict):
            raise OpenAPICompileError("Schema must be an object")
        result: dict[str, Any] = {}
        for key, value in schema.items():
            if not isinstance(key, str):
                raise OpenAPICompileError("Schema keyword names must be strings")
            if key == "discriminator":
                raise OpenAPICompileError(
                    "OpenAPI discriminator semantics are unsupported; use JSON Schema oneOf"
                )
            if key in _METADATA or key == "nullable" or key.startswith("x-"):
                continue
            if key not in _SCHEMA_KEYWORDS:
                raise OpenAPICompileError(f"Unsupported JSON Schema keyword: {key}")
            if key == "$schema":
                if value not in {
                    "https://json-schema.org/draft/2020-12/schema",
                    "https://json-schema.org/draft/2020-12/schema#",
                }:
                    raise OpenAPICompileError("Only JSON Schema draft 2020-12 is supported")
                continue
            if key in {"$id", "$anchor", "$dynamicAnchor"}:
                raise OpenAPICompileError(f"Schema keyword {key} is unsupported")
            if key in _SCHEMA_MAPS:
                if not isinstance(value, dict):
                    raise OpenAPICompileError(f"Schema {key} must be an object")
                result[key] = {name: self.schema(item) for name, item in value.items()}
            elif key in _SCHEMA_ARRAYS:
                if not isinstance(value, list):
                    raise OpenAPICompileError(f"Schema {key} must be an array")
                result[key] = [self.schema(item) for item in value]
            elif key in _SCHEMA_SINGLE:
                result[key] = (
                    value if key == "additionalProperties" and isinstance(value, bool) else self.schema(value)
                )
            elif self.is_30 and key in {"exclusiveMinimum", "exclusiveMaximum"}:
                if not isinstance(value, bool):
                    raise OpenAPICompileError(f"OpenAPI 3.0 {key} must be boolean")
                bound = "minimum" if key == "exclusiveMinimum" else "maximum"
                if value:
                    if bound not in schema:
                        raise OpenAPICompileError(f"{key} requires {bound}")
                    result[key] = schema[bound]
            else:
                result[key] = copy.deepcopy(value)
        if self.is_30:
            for key, bound in (("exclusiveMinimum", "minimum"), ("exclusiveMaximum", "maximum")):
                if schema.get(key) is True:
                    result.pop(bound, None)
            if schema.get("nullable") is True:
                if "type" not in result or not isinstance(result["type"], str):
                    raise OpenAPICompileError("OpenAPI 3.0 nullable requires an explicit scalar type")
                result["type"] = [result["type"], "null"]
        elif "nullable" in schema:
            raise OpenAPICompileError("OpenAPI 3.1 uses type: [<type>, null], not nullable")
        if isinstance(result.get("required"), list) and isinstance(schema.get("properties"), dict):
            # OpenAPI readOnly required fields apply to responses, not requests.
            read_only = {
                name
                for name, prop in schema["properties"].items()
                if isinstance(prop, dict) and prop.get("readOnly") is True
            }
            result["required"] = [name for name in result["required"] if name not in read_only]
        try:
            Draft202012Validator.check_schema(result)
        except SchemaError as exc:
            raise OpenAPICompileError(f"Invalid JSON Schema: {exc.message}") from None
        return result


def _name(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OpenAPICompileError("Service and operation names must be non-empty strings")
    return re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_") or "operation"


def _base_url(servers: Any) -> str:
    if not isinstance(servers, list) or not servers or not isinstance(servers[0], dict):
        raise OpenAPICompileError("An explicit base_url or OpenAPI servers[0].url is required")
    server = servers[0]
    value = server.get("url")
    if not isinstance(value, str):
        raise OpenAPICompileError("Server URL must be a string")
    variables = server.get("variables", {})
    if not isinstance(variables, dict):
        raise OpenAPICompileError("Server variables must be an object")
    for key, variable in variables.items():
        if not isinstance(key, str):
            raise OpenAPICompileError("Server variable names must be strings")
        if not isinstance(variable, dict) or "default" not in variable:
            raise OpenAPICompileError(f"Server variable {key} requires a default")
        value = value.replace("{" + key + "}", str(variable["default"]))
    return _validate_base_url(value)


def _validate_base_url(value: str) -> str:
    if (
        not isinstance(value, str)
        or "{" in value
        or "}" in value
        or "\\" in value
        or any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in value)
    ):
        raise OpenAPICompileError("Base URL must be an absolute HTTP(S) URL without unresolved variables")
    try:
        parsed = urlsplit(value)
        valid_port = parsed.port
    except ValueError:
        raise OpenAPICompileError("Invalid base URL") from None
    del valid_port
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise OpenAPICompileError("Base URL must be HTTP(S), without credentials, query, or fragment")
    return value.rstrip("/")


def _parameters(
    compiler: _Compiler, path_item: dict[str, Any], operation: dict[str, Any], path: str
) -> tuple[ParameterSpec, ...]:
    secret_parameters: set[tuple[str, str]] = set()
    components = compiler.document.get("components", {})
    schemes = components.get("securitySchemes", {}) if isinstance(components, dict) else {}
    if isinstance(schemes, dict):
        for raw_scheme in schemes.values():
            scheme = compiler.resolve(raw_scheme)
            if isinstance(scheme, dict) and scheme.get("type") == "apiKey":
                name, location = scheme.get("name"), scheme.get("in")
                if isinstance(name, str) and location in {"query", "header", "cookie"}:
                    secret_parameters.add((name.lower() if location == "header" else name, location))
    combined: dict[tuple[str, str], dict[str, Any]] = {}
    for group in (path_item.get("parameters", []), operation.get("parameters", [])):
        if not isinstance(group, list):
            raise OpenAPICompileError("parameters must be an array")
        seen: set[tuple[str, str]] = set()
        for raw in group:
            parameter = compiler.resolve(raw)
            if not isinstance(parameter, dict):
                raise OpenAPICompileError("Each parameter must be an object")
            name, location = parameter.get("name"), parameter.get("in")
            if not isinstance(name, str) or not name or location not in {"path", "query", "header"}:
                raise OpenAPICompileError("Parameters require a name and supported in: path/query/header")
            if location == "header" and (
                name.lower() in _SECRET_HEADERS or name.lower().startswith(("x-forwarded-", "proxy-", "sec-"))
            ):
                raise OpenAPICompileError(f"Sensitive/transport header parameter is forbidden: {name}")
            if location == "header" and not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
                raise OpenAPICompileError(f"Invalid HTTP header parameter name: {name}")
            identity = (name.lower() if location == "header" else name, location)
            if identity in secret_parameters:
                raise OpenAPICompileError(
                    f"Credential parameter must be configured as egress authentication: {name}"
                )
            if identity in seen:
                raise OpenAPICompileError(f"Duplicate parameter: {name}")
            seen.add(identity)
            combined[identity] = parameter
    parameters = []
    argument_names: set[str] = set()
    for parameter in combined.values():
        name, location = parameter["name"], parameter["in"]
        if name in argument_names:
            raise OpenAPICompileError(f"Ambiguous argument name across parameter locations: {name}")
        argument_names.add(name)
        if "content" in parameter or "schema" not in parameter:
            raise OpenAPICompileError(f"Parameter {name} requires schema (content is unsupported)")
        if parameter.get("allowReserved") or parameter.get("allowEmptyValue"):
            raise OpenAPICompileError(f"Parameter {name}: allowReserved/allowEmptyValue are unsupported")
        schema = compiler.schema(parameter["schema"])
        if not isinstance(schema, dict):
            raise OpenAPICompileError(f"Parameter {name} requires a primitive or primitive-array schema")
        kind = schema.get("type")
        if not isinstance(kind, str) or (kind not in _PRIMITIVES and kind != "array"):
            raise OpenAPICompileError(f"Parameter {name} requires an explicit primitive or array type")
        if kind == "array" and (
            not isinstance(schema.get("items"), dict)
            or not isinstance(schema["items"].get("type"), str)
            or schema["items"].get("type") not in _PRIMITIVES
        ):
            raise OpenAPICompileError(f"Parameter {name}: array items must have a primitive type")
        style = parameter.get("style", "form" if location == "query" else "simple")
        expected_style = "form" if location == "query" else "simple"
        if style != expected_style:
            raise OpenAPICompileError(
                f"Parameter {name}: only {expected_style} style is supported in {location}"
            )
        explode = parameter.get("explode", style == "form")
        if not isinstance(explode, bool):
            raise OpenAPICompileError(f"Parameter {name}: explode must be boolean")
        required = parameter.get("required", False)
        if not isinstance(required, bool) or (location == "path" and not required):
            raise OpenAPICompileError(f"Path parameter {name} must declare required: true")
        if "description" in parameter:
            if not isinstance(parameter["description"], str):
                raise OpenAPICompileError(f"Parameter {name}: description must be a string")
            schema = {**schema, "description": parameter["description"]}
        parameters.append(ParameterSpec(name, location, schema, required, style, explode))
    placeholders = re.findall(r"\{([^{}]+)\}", path)
    if len(placeholders) != len(set(placeholders)):
        raise OpenAPICompileError(f"Duplicate path placeholder in {path}")
    if set(placeholders) != {param.name for param in parameters if param.location == "path"}:
        raise OpenAPICompileError(f"Path parameters do not match path placeholders: {path}")
    return tuple(parameters)


def compile_openapi(
    document: dict[str, Any], service_name: str, base_url: str | None = None
) -> list[CompiledOperation]:
    """Compile each OpenAPI operation to exactly one gateway-namespaced tool.

    Specifications are trusted deployment inputs, not client-supplied data. No
    external references are fetched. Authentication declarations intentionally
    do not become input fields; the gateway injects configured egress secrets.
    """
    if not isinstance(document, dict):
        raise OpenAPICompileError("OpenAPI document must be an object")
    version = document.get("openapi", "")
    if not isinstance(version, str) or not re.fullmatch(r"3\.[01]\.\d+", version):
        raise OpenAPICompileError("Only OpenAPI 3.0.x and 3.1.x are supported")
    paths = document.get("paths")
    if not isinstance(paths, dict):
        raise OpenAPICompileError("OpenAPI paths must be an object")
    compiler = _Compiler(document, version)
    explicit_base = _validate_base_url(base_url) if base_url is not None else None
    namespace = _name(service_name)
    compiled = []
    names: set[str] = set()
    for path, raw_path_item in paths.items():
        if isinstance(path, str) and path.startswith("x-"):
            continue
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or path.startswith("//")
            or "?" in path
            or "#" in path
            or "\\" in path
            or any(ch.isspace() for ch in path)
        ):
            raise OpenAPICompileError(f"Invalid OpenAPI path: {path!r}")
        without_placeholders = re.sub(r"\{[^{}]+\}", "placeholder", path)
        if (
            "{" in without_placeholders
            or "}" in without_placeholders
            or any(segment in {".", ".."} for segment in without_placeholders.split("/"))
        ):
            raise OpenAPICompileError(f"Invalid or dot-segment OpenAPI path: {path}")
        path_item = compiler.resolve(raw_path_item)
        if not isinstance(path_item, dict):
            raise OpenAPICompileError(f"Path item must be an object: {path}")
        for method, raw_operation in path_item.items():
            if method not in _HTTP_METHODS:
                continue
            operation = compiler.resolve(raw_operation)
            if not isinstance(operation, dict):
                raise OpenAPICompileError(f"Operation must be an object: {method} {path}")
            if operation.get("callbacks"):
                raise OpenAPICompileError("OpenAPI callbacks are not supported")
            name = f"{namespace}__{_name(operation.get('operationId', method + '_' + path))}"
            if len(name) > 128:
                raise OpenAPICompileError(f"Tool name exceeds 128 characters: {name}")
            if name in names:
                raise OpenAPICompileError(f"Duplicate normalized tool name: {name}")
            names.add(name)
            url = explicit_base or _base_url(
                operation.get("servers", path_item.get("servers", document.get("servers")))
            )
            parameters = _parameters(compiler, path_item, operation, path)
            properties = {param.name: copy.deepcopy(param.schema) for param in parameters}
            required = [param.name for param in parameters if param.required]
            body_schema = None
            body_required = False
            if "requestBody" in operation:
                if "body" in properties:
                    raise OpenAPICompileError("Parameter named body conflicts with JSON requestBody")
                body = compiler.resolve(operation["requestBody"])
                if (
                    not isinstance(body, dict)
                    or not isinstance(body.get("content"), dict)
                    or "application/json" not in body["content"]
                ):
                    raise OpenAPICompileError("requestBody must provide application/json content")
                media = body["content"]["application/json"]
                if not isinstance(media, dict) or "schema" not in media or media.get("encoding"):
                    raise OpenAPICompileError(
                        "JSON requestBody requires schema and must not specify encoding"
                    )
                body_schema = compiler.schema(media["schema"])
                body_required = body.get("required", False)
                if not isinstance(body_required, bool):
                    raise OpenAPICompileError("requestBody required must be boolean")
                properties["body"] = copy.deepcopy(body_schema)
                if body_required:
                    required.append("body")
            schema = {"type": "object", "properties": properties, "additionalProperties": False}
            if required:
                schema["required"] = required
            summary, description = operation.get("summary", ""), operation.get("description", "")
            if not isinstance(summary, str) or not isinstance(description, str):
                raise OpenAPICompileError("Operation summary/description must be strings")
            text = (
                "\n\n".join(part for part in (summary.strip(), description.strip()) if part)
                or f"{method.upper()} {path}"
            )
            compiled.append(
                CompiledOperation(
                    name, text, schema, method.upper(), url, path, parameters, body_required, body_schema
                )
            )
    if not compiled:
        raise OpenAPICompileError("OpenAPI document does not contain any supported operations")
    return compiled

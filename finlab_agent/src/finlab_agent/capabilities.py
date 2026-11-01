from __future__ import annotations

import json
import re
import secrets
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from langchain.tools import tool
from langchain_core.tools import BaseTool

from .repository import PriceRepository
from .schemas import CapabilityExecuteInput, CapabilitySearchInput
from .tools import create_finlab_tools


DEFAULT_MANIFEST = Path(__file__).resolve().parents[2] / "contracts" / "capabilities.v1.json"
_WORD = re.compile(r"[a-z0-9_./-]+|[\u3400-\u9fff]{2,}", re.IGNORECASE)


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_id: str
    summary: str
    when_to_use: str
    output_summary: str
    tags: tuple[str, ...]
    limitations: tuple[str, ...]
    implementation_tool: str
    required_data_products: tuple[str, ...]
    risk_class: str
    enabled: bool
    tool: BaseTool

    def public_dict(self, score: int | None = None) -> dict[str, Any]:
        schema = self.tool.args_schema.model_json_schema() if self.tool.args_schema else {}
        payload: dict[str, Any] = {
            "capability_id": self.capability_id,
            "summary": self.summary,
            "when_to_use": self.when_to_use,
            "output_summary": self.output_summary,
            "input_schema": schema,
            "required_data_products": list(self.required_data_products),
            "risk_class": self.risk_class,
            "limitations": list(self.limitations),
        }
        if score is not None:
            payload["match_score"] = score
        return payload


@dataclass(frozen=True)
class _DiscoveryGrant:
    capability_ids: frozenset[str]
    expires_at: float


class CapabilityRegistry:
    """Allow-listed capability discovery and dispatch.

    Manifest rows describe capabilities, but only Python tools explicitly supplied to
    this registry can execute. Search grants are short-lived and restrict execution to
    the capabilities returned by that search.
    """

    def __init__(
        self,
        definitions: list[CapabilityDefinition],
        catalog_version: str,
        grant_ttl_seconds: int = 900,
    ):
        ids = [item.capability_id for item in definitions]
        if len(ids) != len(set(ids)):
            raise ValueError("capability_id 不可重複")
        self.catalog_version = catalog_version
        self._definitions = {
            item.capability_id: item for item in definitions if item.enabled
        }
        self._grant_ttl_seconds = grant_ttl_seconds
        self._grants: dict[str, _DiscoveryGrant] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_manifest(
        cls,
        manifest_path: str | Path,
        tools: list[BaseTool],
    ) -> "CapabilityRegistry":
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        tools_by_name = {item.name: item for item in tools}
        definitions: list[CapabilityDefinition] = []
        for row in payload["capabilities"]:
            implementation = row["implementation_tool"]
            if implementation not in tools_by_name:
                raise ValueError(
                    f"Capability {row['capability_id']} 找不到實作工具 {implementation}"
                )
            definitions.append(
                CapabilityDefinition(
                    capability_id=row["capability_id"],
                    summary=row["summary"],
                    when_to_use=row["when_to_use"],
                    output_summary=row["output_summary"],
                    tags=tuple(row.get("tags", [])),
                    limitations=tuple(row.get("limitations", [])),
                    implementation_tool=implementation,
                    required_data_products=tuple(row.get("required_data_products", [])),
                    risk_class=row.get("risk_class", "read_only"),
                    enabled=bool(row.get("enabled", False)),
                    tool=tools_by_name[implementation],
                )
            )
        return cls(definitions, payload["catalog_version"])

    @property
    def capability_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))

    def _score(self, definition: CapabilityDefinition, query: str) -> int:
        normalized = query.casefold().strip()
        searchable = " ".join(
            (
                definition.capability_id,
                definition.summary,
                definition.when_to_use,
                *definition.tags,
            )
        ).casefold()
        score = 0
        for tag in definition.tags:
            normalized_tag = tag.casefold()
            if normalized_tag in normalized or normalized in normalized_tag:
                score += 8
        for token in set(_WORD.findall(normalized)):
            if len(token) >= 2 and token in searchable:
                score += 2
        if normalized in searchable:
            score += 5
        return score

    def search(self, query: str, max_results: int = 5) -> dict[str, Any]:
        args = CapabilitySearchInput(query=query, max_results=max_results)
        ranked = sorted(
            (
                (self._score(definition, args.query), definition)
                for definition in self._definitions.values()
            ),
            key=lambda item: (-item[0], item[1].capability_id),
        )
        matched = [item for item in ranked if item[0] > 0][: args.max_results]
        if not matched:
            matched = ranked[: args.max_results]

        now = time.monotonic()
        token = secrets.token_urlsafe(18)
        grant = _DiscoveryGrant(
            capability_ids=frozenset(item.capability_id for _, item in matched),
            expires_at=now + self._grant_ttl_seconds,
        )
        with self._lock:
            self._grants = {
                key: value for key, value in self._grants.items() if value.expires_at > now
            }
            self._grants[token] = grant
            if len(self._grants) > 2048:
                oldest = min(self._grants, key=lambda key: self._grants[key].expires_at)
                self._grants.pop(oldest, None)

        return {
            "catalog_version": self.catalog_version,
            "capability_token": token,
            "expires_in_seconds": self._grant_ttl_seconds,
            "matches": [definition.public_dict(score) for score, definition in matched],
        }

    def execute(
        self,
        capability_token: str,
        capability_id: str,
        arguments: Mapping[str, Any],
    ) -> dict[str, Any]:
        args = CapabilityExecuteInput(
            capability_token=capability_token,
            capability_id=capability_id,
            arguments=dict(arguments),
        )
        now = time.monotonic()
        with self._lock:
            grant = self._grants.get(args.capability_token)
        if grant is None or grant.expires_at <= now:
            raise ValueError("capability_token 無效或已過期；請重新搜尋能力")
        if args.capability_id not in grant.capability_ids:
            raise ValueError("此 capability 不在該次搜尋結果中；請重新搜尋")
        definition = self._definitions.get(args.capability_id)
        if definition is None:
            raise ValueError("capability 未啟用")
        result = definition.tool.invoke(args.arguments)
        return {
            "status": "ok",
            "catalog_version": self.catalog_version,
            "capability_id": definition.capability_id,
            "implementation_tool": definition.implementation_tool,
            "result": result,
        }


def build_capability_registry(
    repository: PriceRepository,
    manifest_path: str | Path = DEFAULT_MANIFEST,
) -> CapabilityRegistry:
    return CapabilityRegistry.from_manifest(
        manifest_path=manifest_path,
        tools=create_finlab_tools(repository),
    )


def create_capability_gateway_tools(registry: CapabilityRegistry):
    @tool("search_capabilities", args_schema=CapabilitySearchInput)
    def search_capabilities(query: str, max_results: int = 5) -> dict[str, Any]:
        """搜尋目前已啟用且允許 Agent 使用的研究能力。

        每個研究步驟執行前都要先用自然語言搜尋。結果包含能力 ID、輸入 schema、
        依賴的 Gold data products、風險類別與短效 token。
        """
        return registry.search(query=query, max_results=max_results)

    @tool("execute_capability", args_schema=CapabilityExecuteInput)
    def execute_capability(
        capability_token: str,
        capability_id: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """執行剛由 search_capabilities 找到的已註冊能力。

        capability_id、token 與 arguments 必須完全依搜尋結果；此入口不接受任意
        Python、SQL、資料表名稱、模型 URI 或未註冊工具。
        """
        return registry.execute(capability_token, capability_id, arguments)

    return [search_capabilities, execute_capability]

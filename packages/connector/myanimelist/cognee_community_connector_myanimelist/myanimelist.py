"""MyAnimeList dlt source (Jikan v4 public API).

Public, no auth. Rate: 3 req/sec, 60/min; server caches 24h.
Throttled with sleep(0.4) between requests.
Pagination via pagination.has_next_page. Retry on 429 honoring Retry-After.
Content is markdown (not raw JSON). Per-item try/except with print.
DOCUMENT_SOURCE_ATTR set on source object (mirrors github.py exactly)."""
from __future__ import annotations

import os
import time
from typing import Any

import dlt
import httpx

try:
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR
except Exception:
    DOCUMENT_SOURCE_ATTR = "dlt_source"

API_ROOT = "https://api.jikan.moe/v4"
USER_AGENT = "cognee-myanimelist-connector"


def _wait_retry_after(resp: httpx.Response) -> float:
    ra = resp.headers.get("Retry-After")
    if ra and ra.isdigit():
        return float(ra)
    return 2.0


def _request(client: httpx.Client, url: str) -> Any:
    last_exc: Exception | None = None
    for _ in range(4):
        time.sleep(0.4)  # Jikan allows 3 req/sec
        try:
            resp = client.get(url, timeout=30.0)
        except httpx.HTTPError as exc:
            last_exc = exc
            time.sleep(0.4)
            continue
        if resp.status_code == 429:
            wait = _wait_retry_after(resp)
            time.sleep(wait)
            time.sleep(0.4)
            last_exc = httpx.HTTPStatusError(resp.text, response=resp, request=resp.request)
            continue
        resp.raise_for_status()
        if "application/json" in resp.headers.get("content-type", ""):
            return resp.json()
        return resp.text or ""
    if last_exc:
        raise last_exc
    raise RuntimeError("request failed")


def _paginate_list(client: httpx.Client, url: str) -> list[Any]:
    out: list[Any] = []
    page = 1
    while True:
        time.sleep(0.4)
        paged_url = f"{url}&page={page}" if "?" in url else f"{url}?page={page}"
        resp = client.get(paged_url, timeout=30.0)
        if resp.status_code == 429:
            wait = _wait_retry_after(resp)
            time.sleep(wait)
            resp = client.get(paged_url, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        if isinstance(items, list):
            out.extend(items)
        if not data.get("pagination", {}).get("has_next_page", False):
            break
        page += 1
    return out


def _anime_to_row(anime: dict[str, Any], list_status: dict[str, Any]) -> dict[str, Any]:
    mal_id = anime.get("mal_id")
    title = anime.get("title") or anime.get("title_english") or "Unknown"
    url = anime.get("url", f"https://myanimelist.net/anime/{mal_id}")
    synopsis = (anime.get("synopsis") or "").strip()
    score = anime.get("score")
    genres = ", ".join(g.get("name", "") for g in anime.get("genres", []) if isinstance(g, dict))
    studios = ", ".join(s.get("name", "") for s in anime.get("studios", []) if isinstance(s, dict))
    status = list_status.get("status", "")
    my_score = list_status.get("score", 0)
    eps_watched = list_status.get("num_episodes_watched", 0)
    content_lines = [f"## {title}"]
    content_lines.append("**My list**")
    content_lines.append(f"- Status: {status}")
    content_lines.append(f"- My score: {my_score}/10")
    content_lines.append(f"- Episodes watched: {eps_watched}")
    if synopsis:
        content_lines.append("### Synopsis")
        content_lines.append(synopsis)
    meta_parts = []
    if genres:
        meta_parts.append(f"Genres: {genres}")
    if studios:
        meta_parts.append(f"Studios: {studios}")
    if score is not None:
        meta_parts.append(f"MAL score: {score}")
    if meta_parts:
        content_lines.append("---")
        content_lines.append(" ".join(meta_parts))
    return {
        "id": f"anime:{mal_id}",
        "url": url,
        "title": title,
        "content": "\n".join(content_lines),
    }


def myanimelist_source(username: str | None = None):
    username = username or os.environ.get("MAL_USERNAME")
    if not username:
        raise ValueError("MyAnimeList username required: pass username= or set MAL_USERNAME.")

    client = httpx.Client(
        base_url=API_ROOT,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )

    @dlt.resource(name="myanimelist_documents", primary_key="id", write_disposition="replace")
    def myanimelist_documents():
        list_url = f"{API_ROOT}/users/{username}/animelist?limit=25"
        list_items = _paginate_list(client, list_url)
        for item in list_items:
            anime_dict = item.get("anime", {})
            list_status = item.get("list_status", {})
            mal_id = anime_dict.get("mal_id")
            if not mal_id:
                continue
            # Fetch full anime details for synopsis/genres/studios
            full_url = f"{API_ROOT}/anime/{mal_id}/full"
            full_data = {}
            try:
                full_data = _request(client, full_url)
            except Exception as exc:
                print(f"[myanimelist] full-detail fetch failed for anime:{mal_id}: {exc}")
            anime_full = full_data.get("data", anime_dict) if isinstance(full_data, dict) else anime_dict
            # Enrich anime_dict with full details but keep list_status separate
            merged_anime = dict(anime_dict)
            for key in ("synopsis", "score", "genres", "studios", "title_english"):
                if key in anime_full:
                    merged_anime[key] = anime_full[key]
            try:
                yield _anime_to_row(merged_anime, list_status)
            except Exception as exc:
                print(f"[myanimelist] skipping anime:{mal_id}: {exc}")

    @dlt.source(name="myanimelist")
    def _myanimelist():
        return myanimelist_documents

    source = _myanimelist()
    setattr(source, DOCUMENT_SOURCE_ATTR, "myanimelist")
    return source

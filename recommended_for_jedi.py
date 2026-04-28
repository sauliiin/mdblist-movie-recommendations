#!/usr/bin/env python3
"""Daily MDBList recommender for the "Recommended for Jedi" static list.

The script builds a taste profile from the first 15 movies in the user's
Last Watched list, ranks candidate movies, excludes watched movies, and updates
the target MDBList list incrementally.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


API_BASE = "https://api.mdblist.com"
WEB_BASE = "https://mdblist.com"

USERNAME = "mestreyodarossi"
LAST_WATCHED_SLUG = "last-watched"
TARGET_LIST_NAME = "Recommended for Jedi"

BLOCKED_SOURCE_LIST_SLUGS = {
    "trending-movies",
    "lastest-movie-releases",
    "combina-com-voce",
    "surprise-me",
    "fast-horror",
}

PROFILE_SIZE = 7
TARGET_SIZE = 100
MIN_DAILY_SWAPS = 50
RAW_CANDIDATE_LIMIT = 30_000
ENRICH_LIMIT = 3_000

MIN_IMDB_VALUE = 5.2
MIN_IMDB_VOTES = 1_000
MIN_RUNTIME = 70
MAX_RUNTIME = 180

REPORT_DIR = Path("reports")
CACHE_FILE = Path(".cache/mdblist_movies.json")

BASIC_KEYWORDS = {
    "has-trailer",
    "trailer",
    "dvd",
    "blu-ray",
    "2k-blu-ray",
    "4k-blu-ray",
    "4k",
    "4k-ultra-hd",
    "dolby-vision",
    "dolby-atmos",
    "fresh",
    "certified-fresh",
    "certified-hot",
    "rotten",
    "belongs-to-collection",
    "collection-follow-up",
    "first-in-collection",
    "metacritic-must-see",
    "oscar-winner",
    "oscar-nominated",
    "golden-globe-nominated",
    "golden-globe-winner",
    "f-rated",
    "triple-f-rated",
    "title-directed-by-female",
    "written-by-director",
    "one-word-title",
    "two-word-title",
    "three-word-title",
    "four-word-title",
    "color-in-title",
    "2010s",
    "2010s-movie",
    "2020s",
    "2020s-movie",
}


class MDBListError(RuntimeError):
    pass


@dataclass(frozen=True)
class MovieKey:
    tmdb: int | None = None
    imdb: str | None = None
    mdblist: str | None = None

    @property
    def stable(self) -> str:
        if self.tmdb is not None:
            return f"tmdb:{self.tmdb}"
        if self.imdb:
            return f"imdb:{self.imdb}"
        if self.mdblist:
            return f"mdblist:{self.mdblist}"
        return "unknown"


@dataclass
class Candidate:
    key: MovieKey
    title: str = ""
    year: int | None = None
    sources: set[str] | None = None

    def add_source(self, source: str) -> None:
        if self.sources is None:
            self.sources = set()
        self.sources.add(source)


@dataclass
class FineTuning:
    """User-level fine-tuning filters.

    Any field left as ``None`` (or empty list) causes the script to fall back
    to its built-in default behaviour for that filter.
    """

    # Genres whose movies must be completely excluded from results.
    excluded_genres: list[str] = field(default_factory=list)

    # Keywords whose movies must be completely excluded from results.
    excluded_keywords: list[str] = field(default_factory=list)

    # Actor names whose movies must be completely excluded from results.
    excluded_actors: list[str] = field(default_factory=list)

    # IMDB rating bounds.  ``None`` means "use script default".
    imdb_min: float | None = None
    imdb_max: float | None = None

    # Minimum IMDB votes.  ``None`` means "use script default".
    imdb_min_votes: int | None = None

    # Release year bounds.  ``None`` means no limit (current default).
    year_min: int | None = None
    year_max: int | None = None

    # ---- helpers -----------------------------------------------------------

    @property
    def effective_imdb_min(self) -> float:
        return self.imdb_min if self.imdb_min is not None else MIN_IMDB_VALUE

    @property
    def effective_imdb_max(self) -> float | None:
        return self.imdb_max  # None means no upper bound (current default)

    @property
    def effective_imdb_min_votes(self) -> int:
        return self.imdb_min_votes if self.imdb_min_votes is not None else MIN_IMDB_VOTES

    @property
    def _excluded_genres_lower(self) -> set[str]:
        return {g.strip().lower() for g in self.excluded_genres}

    @property
    def _excluded_keywords_lower(self) -> set[str]:
        return {k.strip().lower() for k in self.excluded_keywords}

    @property
    def _excluded_actors_lower(self) -> set[str]:
        return {a.strip().lower() for a in self.excluded_actors}

    def has_excluded_genre(self, genres: list[str]) -> bool:
        if not self.excluded_genres:
            return False
        lower = self._excluded_genres_lower
        return any(g.strip().lower() in lower for g in genres)

    def has_excluded_keyword(self, keywords: list[str]) -> bool:
        if not self.excluded_keywords:
            return False
        lower = self._excluded_keywords_lower
        return any(k.strip().lower() in lower for k in keywords)

    def has_excluded_actor(self, movie: dict[str, Any]) -> bool:
        if not self.excluded_actors:
            return False
        lower = self._excluded_actors_lower
        # MDBList movie details may include actors under "actors" or "cast"
        actors = movie.get("actors") or movie.get("cast") or []
        if isinstance(actors, str):
            # Sometimes returned as comma-separated string
            actors = [a.strip() for a in actors.split(",")]
        for actor in actors:
            name = actor.get("name", actor) if isinstance(actor, dict) else str(actor)
            if str(name).strip().lower() in lower:
                return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "excluded_genres": self.excluded_genres,
            "excluded_keywords": self.excluded_keywords,
            "excluded_actors": self.excluded_actors,
            "imdb_min": self.imdb_min,
            "imdb_max": self.imdb_max,
            "imdb_min_votes": self.imdb_min_votes,
            "effective_imdb_min": self.effective_imdb_min,
            "effective_imdb_max": self.effective_imdb_max,
            "effective_imdb_min_votes": self.effective_imdb_min_votes,
            "year_min": self.year_min,
            "year_max": self.year_max,
        }


def log_step(message: str) -> None:
    timestamp = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()


def today_seed() -> str:
    return dt.date.today().isoformat()


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def median(values: list[float], default: float) -> float:
    return statistics.median(values) if values else default


def mean(values: list[float], default: float) -> float:
    return statistics.mean(values) if values else default


def pstdev(values: list[float], default: float) -> float:
    if len(values) < 2:
        return default
    return max(statistics.pstdev(values), default)


def gaussian_distance(value: float | None, samples: list[float], scale: float) -> float:
    if value is None or not samples:
        return 0.0
    return sum(math.exp(-abs(value - sample) / scale) for sample in samples) / len(samples)


def hash_jitter(seed: str, stable_key: str, amplitude: float = 0.02) -> float:
    digest = hashlib.sha256(f"{seed}:{stable_key}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
    return (value - 0.5) * 2 * amplitude


def normalize_ids(obj: dict[str, Any]) -> MovieKey:
    ids = obj.get("ids") or {}
    tmdb = (
        ids.get("tmdb")
        or ids.get("tmdbid")
        or obj.get("tmdb")
        or obj.get("tmdb_id")
        or obj.get("id")
    )
    imdb = ids.get("imdb") or ids.get("imdbid") or obj.get("imdb") or obj.get("imdb_id")
    mdblist = ids.get("mdblist") or obj.get("mdblist")
    try:
        tmdb_int = int(tmdb) if tmdb is not None else None
    except (TypeError, ValueError):
        tmdb_int = None
    return MovieKey(tmdb=tmdb_int, imdb=imdb, mdblist=mdblist)


def movie_identity_set(movie: dict[str, Any]) -> set[str]:
    key = normalize_ids(movie)
    values = set()
    if key.tmdb is not None:
        values.add(f"tmdb:{key.tmdb}")
    if key.imdb:
        values.add(f"imdb:{key.imdb}")
    if key.mdblist:
        values.add(f"mdblist:{key.mdblist}")
    return values


class MDBListClient:
    def __init__(self, api_key: str, timeout: int = 30, sleep: float = 0.05) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.sleep = sleep

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        params = dict(params or {})
        params["apikey"] = self.api_key
        query = urllib.parse.urlencode(params, doseq=True)
        return f"{API_BASE}{path}?{query}"

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        url = self._url(path, params)
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; RecommendedForJedi/1.0)",
        }
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise MDBListError(f"{method} {path} failed: HTTP {exc.code}: {error_body}") from exc
        except urllib.error.URLError as exc:
            raise MDBListError(f"{method} {path} failed: {exc}") from exc
        finally:
            if self.sleep:
                time.sleep(self.sleep)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MDBListError(f"{method} {path} returned non-JSON: {raw[:300]}") from exc

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self.request("GET", path, params=params)

    def post(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("POST", path, payload=payload)

    def list_items(
        self,
        path: str,
        *,
        limit: int = 100,
        extra_params: dict[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        offset = 0
        items: list[dict[str, Any]] = []
        while True:
            params = {"limit": limit, "offset": offset}
            if extra_params:
                params.update(extra_params)
            data = self.get(path, params)
            batch = data.get("movies") if isinstance(data, dict) else None
            if batch is None and isinstance(data, dict):
                batch = data.get("items")
            if not batch:
                break
            items.extend(batch)
            if max_items and len(items) >= max_items:
                return items[:max_items]
            if len(batch) < limit:
                break
            offset += limit
        return items

    def catalog_movies(
        self,
        *,
        genre: str | None = None,
        sort: str = "score",
        sort_order: str = "desc",
        max_items: int = 500,
    ) -> list[dict[str, Any]]:
        cursor = None
        items: list[dict[str, Any]] = []
        while len(items) < max_items:
            params: dict[str, Any] = {
                "limit": 100,
                "sort": sort,
                "sort_order": sort_order,
            }
            if genre:
                params["genre"] = genre
            if cursor:
                params["cursor"] = cursor
            data = self.get("/catalog/movie", params)
            batch = data.get("movies") or []
            items.extend(batch)
            pagination = data.get("pagination") or {}
            cursor = pagination.get("next_cursor")
            if not cursor or not pagination.get("has_more"):
                break
        return items[:max_items]

    def search_movies(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        data = self.get("/search/movie", {"query": query, "limit": limit, "score_min": 0})
        return data.get("search") or []

    def movie_details(self, tmdb_id: int) -> dict[str, Any]:
        return self.get(f"/tmdb/movie/{tmdb_id}", {"append_to_response": "keyword"})


class MovieCache:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, dict[str, Any]] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                self.data = json.load(handle)

    def get(self, tmdb_id: int) -> dict[str, Any] | None:
        return self.data.get(str(tmdb_id))

    def set(self, tmdb_id: int, movie: dict[str, Any]) -> None:
        self.data[str(tmdb_id)] = {"cached_at": now_utc(), "movie": movie}

    def movie(self, tmdb_id: int) -> dict[str, Any] | None:
        entry = self.get(tmdb_id)
        if not entry:
            return None
        return entry.get("movie")

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(self.data, handle, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.replace(self.path)


def fetch_movie_details(client: MDBListClient, cache: MovieCache, tmdb_id: int) -> dict[str, Any]:
    cached = cache.movie(tmdb_id)
    if cached:
        return cached
    movie = client.movie_details(tmdb_id)
    cache.set(tmdb_id, movie)
    return movie


def imdb_rating(movie: dict[str, Any]) -> tuple[float | None, int | None]:
    for rating in movie.get("ratings") or []:
        if rating.get("source") == "imdb":
            value = rating.get("value")
            votes = rating.get("votes")
            try:
                return float(value), int(votes)
            except (TypeError, ValueError):
                return None, None
    return None, None


def movie_keywords(movie: dict[str, Any]) -> list[dict[str, Any]]:
    return [kw for kw in movie.get("keywords") or [] if kw.get("name")]


def clean_keyword_name(name: str) -> str:
    return name.strip().lower()


def movie_genres(movie: dict[str, Any]) -> list[str]:
    return [str(genre.get("title", "")).strip() for genre in movie.get("genres") or [] if genre.get("title")]


def build_profile(recent_movies: list[dict[str, Any]], seed: str) -> dict[str, Any]:
    keyword_counts: dict[str, int] = {}
    keyword_ids: dict[str, int] = {}
    genre_counts: dict[str, int] = {}
    cert_counts: dict[str, int] = {}
    country_counts: dict[str, int] = {}
    imdb_values: list[float] = []
    imdb_votes: list[int] = []
    years: list[float] = []
    runtimes: list[float] = []
    ages: list[float] = []

    for movie in recent_movies:
        value, votes = imdb_rating(movie)
        if value is not None:
            imdb_values.append(value)
        if votes is not None:
            imdb_votes.append(votes)
        if movie.get("year") is not None:
            years.append(float(movie["year"]))
        if movie.get("runtime") is not None:
            runtimes.append(float(movie["runtime"]))
        if movie.get("age_rating") is not None:
            ages.append(float(movie["age_rating"]))

        seen_keywords: set[str] = set()
        for keyword in movie_keywords(movie):
            name = clean_keyword_name(keyword["name"])
            if name in BASIC_KEYWORDS or name in seen_keywords:
                continue
            seen_keywords.add(name)
            keyword_counts[name] = keyword_counts.get(name, 0) + 1
            if keyword.get("id") is not None:
                keyword_ids.setdefault(name, int(keyword["id"]))

        for genre in set(movie_genres(movie)):
            genre_counts[genre] = genre_counts.get(genre, 0) + 1

        certification = movie.get("certification")
        if certification:
            cert_counts[str(certification)] = cert_counts.get(str(certification), 0) + 1
        country = movie.get("country")
        if country:
            country_counts[str(country).lower()] = country_counts.get(str(country).lower(), 0) + 1

    repeated_keywords = {kw: count for kw, count in keyword_counts.items() if count >= 2}
    rng = random.Random(seed)
    daily_keywords = sorted(repeated_keywords)
    rng.shuffle(daily_keywords)
    daily_keywords = set(daily_keywords[: max(1, min(30, len(daily_keywords) // 5 or len(daily_keywords)))])

    return {
        "generated_from": [
            {
                "title": movie.get("title"),
                "year": movie.get("year"),
                "tmdb": normalize_ids(movie).tmdb,
            }
            for movie in recent_movies
        ],
        "keyword_weights": dict(sorted(repeated_keywords.items(), key=lambda item: (-item[1], item[0]))),
        "keyword_ids": {kw: keyword_ids[kw] for kw in repeated_keywords if kw in keyword_ids},
        "daily_keywords": sorted(daily_keywords),
        "genre_weights": dict(sorted(genre_counts.items(), key=lambda item: (-item[1], item[0]))),
        "certification_weights": cert_counts,
        "country_weights": country_counts,
        "imdb_mean": mean(imdb_values, 6.3),
        "imdb_std": pstdev(imdb_values, 0.6),
        "imdb_votes_median": median([float(v) for v in imdb_votes], 60_000),
        "years": years,
        "year_mean": mean(years, 2018),
        "runtimes": runtimes,
        "runtime_mean": mean(runtimes, 100),
        "age_ratings": ages,
        "age_mean": mean(ages, 16),
    }


def bayesian_imdb(value: float, votes: int, profile: dict[str, Any]) -> float:
    c = float(profile["imdb_mean"])
    m = float(profile["imdb_votes_median"])
    return (votes / (votes + m)) * value + (m / (votes + m)) * c


def passes_filters(
    movie: dict[str, Any],
    excluded_identities: set[str],
    fine_tuning: FineTuning | None = None,
) -> tuple[bool, list[str]]:
    ft = fine_tuning or FineTuning()
    reasons: list[str] = []
    if (movie.get("type") or "movie") != "movie":
        reasons.append("not_movie")
    if movie_identity_set(movie) & excluded_identities:
        reasons.append("excluded")
    runtime = movie.get("runtime")
    if runtime is None or runtime < MIN_RUNTIME or runtime > MAX_RUNTIME:
        reasons.append("runtime")
    value, votes = imdb_rating(movie)
    if value is None or value < ft.effective_imdb_min:
        reasons.append("imdb_value")
    if ft.effective_imdb_max is not None and value is not None and value > ft.effective_imdb_max:
        reasons.append("imdb_value_max")
    if votes is None or votes < ft.effective_imdb_min_votes:
        reasons.append("imdb_votes")
    # Fine-tuning: excluded genres
    if ft.has_excluded_genre(movie_genres(movie)):
        reasons.append("excluded_genre")
    # Fine-tuning: excluded keywords
    kw_names = [clean_keyword_name(kw["name"]) for kw in movie_keywords(movie)]
    if ft.has_excluded_keyword(kw_names):
        reasons.append("excluded_keyword")
    # Fine-tuning: excluded actors
    if ft.has_excluded_actor(movie):
        reasons.append("excluded_actor")
    # Fine-tuning: release year bounds
    year = movie.get("year")
    if year is not None:
        if ft.year_min is not None and int(year) < ft.year_min:
            reasons.append("year_min")
        if ft.year_max is not None and int(year) > ft.year_max:
            reasons.append("year_max")
    return not reasons, reasons


def score_movie(movie: dict[str, Any], profile: dict[str, Any], seed: str) -> dict[str, Any]:
    keyword_weights = profile["keyword_weights"]
    genre_weights = profile["genre_weights"]
    daily_keywords = set(profile["daily_keywords"])

    candidate_keywords = {
        clean_keyword_name(keyword["name"])
        for keyword in movie_keywords(movie)
        if clean_keyword_name(keyword["name"]) not in BASIC_KEYWORDS
    }
    candidate_genres = set(movie_genres(movie))

    keyword_total = sum(keyword_weights.values()) or 1
    keyword_match = sum(weight for keyword, weight in keyword_weights.items() if keyword in candidate_keywords)
    keyword_score = min(1.0, keyword_match / keyword_total * 3.0)

    daily_total = sum(keyword_weights.get(keyword, 0) for keyword in daily_keywords) or 1
    daily_match = sum(keyword_weights.get(keyword, 0) for keyword in daily_keywords if keyword in candidate_keywords)
    daily_bonus = min(0.06, (daily_match / daily_total) * 0.06)

    genre_total = sum(genre_weights.values()) or 1
    genre_match = sum(weight for genre, weight in genre_weights.items() if genre in candidate_genres)
    genre_score = min(1.0, genre_match / genre_total * 2.0)

    value, votes = imdb_rating(movie)
    if value is not None and votes is not None:
        bayes = bayesian_imdb(value, votes, profile)
        imdb_score = math.exp(-((bayes - float(profile["imdb_mean"])) ** 2) / (2 * float(profile["imdb_std"]) ** 2))
    else:
        bayes = None
        imdb_score = 0.0

    year_score = gaussian_distance(float(movie["year"]) if movie.get("year") else None, profile["years"], 6.0)
    runtime_score = gaussian_distance(
        float(movie["runtime"]) if movie.get("runtime") else None,
        profile["runtimes"],
        22.0,
    )
    age_score = gaussian_distance(
        float(movie["age_rating"]) if movie.get("age_rating") is not None else None,
        profile["age_ratings"],
        3.0,
    )
    certification = str(movie.get("certification") or "")
    certification_score = 0.0
    if certification:
        certification_score = profile["certification_weights"].get(certification, 0) / PROFILE_SIZE
    age_cert_score = (age_score * 0.65) + (certification_score * 0.35)

    country = str(movie.get("country") or "").lower()
    country_score = profile["country_weights"].get(country, 0) / PROFILE_SIZE if country else 0.0

    key = normalize_ids(movie)
    jitter = hash_jitter(seed, key.stable)
    total = (
        0.40 * keyword_score
        + 0.20 * genre_score
        + 0.15 * imdb_score
        + 0.10 * year_score
        + 0.05 * runtime_score
        + 0.07 * age_cert_score
        + 0.03 * country_score
        + daily_bonus
        + jitter
    )

    return {
        "score": round(total, 6),
        "components": {
            "keywords": round(keyword_score, 6),
            "genres": round(genre_score, 6),
            "imdb": round(imdb_score, 6),
            "year": round(year_score, 6),
            "runtime": round(runtime_score, 6),
            "age_certification": round(age_cert_score, 6),
            "country": round(country_score, 6),
            "daily_bonus": round(daily_bonus, 6),
            "jitter": round(jitter, 6),
        },
        "matched_keywords": sorted(candidate_keywords & set(keyword_weights)),
        "matched_genres": sorted(candidate_genres & set(genre_weights)),
        "imdb_bayesian": round(bayes, 6) if bayes is not None else None,
    }


def add_candidate(candidates: dict[str, Candidate], raw: dict[str, Any], source: str) -> None:
    key = normalize_ids(raw)
    if key.tmdb is None:
        return
    stable = key.stable
    if stable not in candidates:
        candidates[stable] = Candidate(
            key=key,
            title=str(raw.get("title") or ""),
            year=raw.get("year") or raw.get("release_year"),
            sources=set(),
        )
    candidates[stable].add_source(source)


def fetch_public_keyword_candidates(keyword_id: int, source: str) -> list[dict[str, Any]]:
    url = f"{WEB_BASE}/movies/?q_tag={keyword_id}&mediatype=movie"
    request = urllib.request.Request(url, headers={"User-Agent": "recommended-for-jedi/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            html = response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError:
        return []
    results = []
    for match in re.finditer(r"themoviedb\.org/movie/(\d+)", html):
        results.append({"id": int(match.group(1)), "ids": {"tmdb": int(match.group(1))}, "source": source})
    return results


def build_candidate_pool(
    client: MDBListClient,
    profile: dict[str, Any],
    excluded_identities: set[str],
    args: argparse.Namespace,
    fine_tuning: FineTuning | None = None,
) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}

    log_step("Montando pool de candidatos: listas do usuario")

    user_lists = client.get("/lists/user")
    for user_list in user_lists:
        slug = user_list.get("slug")
        if slug in {LAST_WATCHED_SLUG, slugify(TARGET_LIST_NAME), *BLOCKED_SOURCE_LIST_SLUGS}:
            continue
        mediatype = user_list.get("mediatype")
        if mediatype not in {None, "movie"}:
            continue
        try:
            items = client.list_items(
                f"/lists/{user_list['id']}/items",
                max_items=min(700, args.max_raw_candidates),
                extra_params={"unified": "false"},
            )
        except MDBListError:
            continue
        for item in items:
            add_candidate(candidates, item, f"user_list:{slug}")
        if len(candidates) >= args.max_raw_candidates:
            log_step(f"Pool de candidatos atingiu limite ({len(candidates)}) nas listas do usuario")
            return candidates

    ft = fine_tuning or FineTuning()
    excluded_genres_lower = ft._excluded_genres_lower
    top_genres = [
        genre
        for genre, _ in sorted(profile["genre_weights"].items(), key=lambda item: -item[1])[:6]
        if genre.strip().lower() not in excluded_genres_lower
    ]
    log_step(f"Expandindo por catalogo de generos: {', '.join(top_genres) if top_genres else 'nenhum'}")
    for genre in top_genres:
        for sort in ("score", "imdbrating", "imdbpopular", "imdbvotes", "released"):
            try:
                items = client.catalog_movies(genre=genre.lower(), sort=sort, max_items=250)
            except MDBListError:
                continue
            for item in items:
                add_candidate(candidates, item, f"catalog:{genre}:{sort}")
            if len(candidates) >= args.max_raw_candidates:
                log_step(f"Pool de candidatos atingiu limite ({len(candidates)}) no catalogo por genero")
                return candidates

    log_step("Expandindo por catalogo global")
    for sort in ("score", "score_average", "imdbrating", "imdbpopular", "imdbvotes", "tmdbpopular", "released"):
        try:
            items = client.catalog_movies(sort=sort, max_items=400)
        except MDBListError:
            continue
        for item in items:
            add_candidate(candidates, item, f"catalog:all:{sort}")
        if len(candidates) >= args.max_raw_candidates:
            log_step(f"Pool de candidatos atingiu limite ({len(candidates)}) no catalogo global")
            return candidates

    keyword_ids = profile["keyword_ids"]
    keyword_items = sorted(profile["keyword_weights"].items(), key=lambda item: (-item[1], item[0]))
    log_step("Expandindo por palavras-chave publicas")
    for keyword, _weight in keyword_items[:80]:
        keyword_id = keyword_ids.get(keyword)
        if keyword_id is None:
            continue
        for item in fetch_public_keyword_candidates(keyword_id, f"keyword:{keyword}"):
            add_candidate(candidates, item, f"keyword:{keyword}")
        if len(candidates) >= args.max_raw_candidates:
            log_step(f"Pool de candidatos atingiu limite ({len(candidates)}) nas palavras-chave publicas")
            return candidates

    log_step("Expandindo por busca textual")
    for keyword, _weight in keyword_items[:40]:
        try:
            items = client.search_movies(keyword, limit=50)
        except MDBListError:
            continue
        for item in items:
            add_candidate(candidates, item, f"search:{keyword}")
        if len(candidates) >= args.max_raw_candidates:
            log_step(f"Pool de candidatos atingiu limite ({len(candidates)}) na busca textual")
            return candidates

    log_step(f"Pool de candidatos finalizado com {len(candidates)} itens")
    return candidates


def rank_candidates(
    client: MDBListClient,
    cache: MovieCache,
    candidates: dict[str, Candidate],
    current_items: list[dict[str, Any]],
    profile: dict[str, Any],
    excluded_identities: set[str],
    args: argparse.Namespace,
    fine_tuning: FineTuning | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    enrich_order = list(candidates.values())
    current_tmdb = {normalize_ids(item).tmdb for item in current_items if normalize_ids(item).tmdb is not None}
    current_candidates = [
        Candidate(key=MovieKey(tmdb=tmdb), sources={"current_target"})
        for tmdb in current_tmdb
        if tmdb is not None and f"tmdb:{tmdb}" not in candidates
    ]
    enrich_order = current_candidates + enrich_order

    ranked: list[dict[str, Any]] = []
    stats = {"enriched": 0, "filtered": 0, "errors": 0}
    seen_tmdb: set[int] = set()
    last_logged_enriched = 0
    log_step(f"Iniciando enriquecimento/ranking de ate {args.max_enrich} candidatos")

    for candidate in enrich_order:
        tmdb_id = candidate.key.tmdb
        if tmdb_id is None or tmdb_id in seen_tmdb:
            continue
        seen_tmdb.add(tmdb_id)
        if stats["enriched"] >= args.max_enrich:
            break
        try:
            movie = fetch_movie_details(client, cache, tmdb_id)
        except MDBListError:
            stats["errors"] += 1
            continue
        stats["enriched"] += 1
        ok, reasons = passes_filters(movie, excluded_identities, fine_tuning)
        if not ok:
            stats["filtered"] += 1
            if stats["enriched"] - last_logged_enriched >= 100:
                log_step(
                    f"Ranking em andamento: enriquecidos={stats['enriched']} filtrados={stats['filtered']} erros={stats['errors']} validos={len(ranked)}"
                )
                last_logged_enriched = stats["enriched"]
            continue
        score = score_movie(movie, profile, args.seed)
        ranked.append(
            {
                "movie": movie,
                "score": score["score"],
                "score_detail": score,
                "sources": sorted(candidate.sources or []),
            }
        )
        if stats["enriched"] - last_logged_enriched >= 100:
            log_step(
                f"Ranking em andamento: enriquecidos={stats['enriched']} filtrados={stats['filtered']} erros={stats['errors']} validos={len(ranked)}"
            )
            last_logged_enriched = stats["enriched"]

    ranked.sort(key=lambda row: row["score"], reverse=True)
    log_step(
        f"Ranking finalizado: enriquecidos={stats['enriched']} filtrados={stats['filtered']} erros={stats['errors']} validos={len(ranked)}"
    )
    return ranked, stats


def choose_target(
    ranked: list[dict[str, Any]],
    current_items: list[dict[str, Any]],
    excluded_identities: set[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current_tmdb = [normalize_ids(item).tmdb for item in current_items if normalize_ids(item).tmdb is not None]
    current_set = {tmdb for tmdb in current_tmdb if tmdb is not None}
    ranked_by_tmdb = {normalize_ids(row["movie"]).tmdb: row for row in ranked if normalize_ids(row["movie"]).tmdb}

    invalid_current: set[int] = set()
    for item in current_items:
        tmdb = normalize_ids(item).tmdb
        if tmdb is None or movie_identity_set(item) & excluded_identities or tmdb not in ranked_by_tmdb:
            if tmdb is not None:
                invalid_current.add(tmdb)

    eligible_current = [tmdb for tmdb in current_tmdb if tmdb is not None and tmdb in ranked_by_tmdb]
    unique_eligible_current = list(dict.fromkeys(eligible_current))

    # Calculate how many to keep to ensure at least MIN_DAILY_SWAPS are changed
    max_keep_allowed = max(0, TARGET_SIZE - MIN_DAILY_SWAPS)
    keep_count = min(max_keep_allowed, len(unique_eligible_current))

    rng = random.Random()
    preserved_tmdb = set(rng.sample(unique_eligible_current, keep_count)) if keep_count > 0 else set()
    target = [ranked_by_tmdb[tmdb] for tmdb in eligible_current if tmdb in preserved_tmdb]

    tmdb_to_replace = current_set - preserved_tmdb
    for row in ranked:
        tmdb = normalize_ids(row["movie"]).tmdb
        if tmdb is None or tmdb in preserved_tmdb or tmdb in tmdb_to_replace:
            continue
        if len(target) >= TARGET_SIZE:
            break
        target.append(row)

    target = target[:TARGET_SIZE]
    target_tmdb = {normalize_ids(row["movie"]).tmdb for row in target if normalize_ids(row["movie"]).tmdb is not None}
    summary = {
        "current_count": len(current_set),
        "target_count": len(target_tmdb),
        "preserved_count": len(current_set & target_tmdb),
        "to_add_count": len(target_tmdb - current_set),
        "to_remove_count": len(current_set - target_tmdb),
        "invalid_current_count": len(invalid_current),
        "minimum_swaps": MIN_DAILY_SWAPS,
        "kept_count": keep_count,
    }
    return target, summary


def ensure_target_list(client: MDBListClient, dry_run: bool) -> dict[str, Any]:
    def pick_target_list(user_lists: list[dict[str, Any]], target_slug: str) -> dict[str, Any] | None:
        static_exact = [
            user_list
            for user_list in user_lists
            if user_list.get("slug") == target_slug and user_list.get("type") == "static"
        ]
        if static_exact:
            return static_exact[0]
        exact = [user_list for user_list in user_lists if user_list.get("slug") == target_slug]
        if exact:
            return exact[0]
        static_by_name = [
            user_list
            for user_list in user_lists
            if user_list.get("name") == TARGET_LIST_NAME and user_list.get("type") == "static"
        ]
        if static_by_name:
            return static_by_name[0]
        by_name = [user_list for user_list in user_lists if user_list.get("name") == TARGET_LIST_NAME]
        if by_name:
            return by_name[0]
        return None

    target_slug = slugify(TARGET_LIST_NAME)
    user_lists = client.get("/lists/user")
    existing = pick_target_list(user_lists, target_slug)
    if existing is not None:
        return existing
    if dry_run:
        return {"id": None, "name": TARGET_LIST_NAME, "slug": target_slug, "items": 0, "created_in_dry_run": True}
    return client.post("/lists/user/add", {"name": TARGET_LIST_NAME, "private": False})


def find_target_list(client: MDBListClient) -> dict[str, Any] | None:
    target_slug = slugify(TARGET_LIST_NAME)
    user_lists = client.get("/lists/user")
    for user_list in user_lists:
        if user_list.get("slug") == target_slug and user_list.get("type") == "static":
            return user_list
    for user_list in user_lists:
        if user_list.get("slug") == target_slug:
            return user_list
    for user_list in user_lists:
        if user_list.get("name") == TARGET_LIST_NAME and user_list.get("type") == "static":
            return user_list
    for user_list in user_lists:
        if user_list.get("name") == TARGET_LIST_NAME:
            return user_list
    return None


def chunked(values: list[dict[str, Any]], size: int = 100) -> list[list[dict[str, Any]]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def apply_delta(
    client: MDBListClient,
    list_id: int,
    to_add: list[int],
    to_remove: list[int],
    dry_run: bool,
) -> dict[str, Any]:
    result = {"added_batches": [], "removed_batches": [], "dry_run": dry_run}
    remove_payloads = [{"tmdb": tmdb} for tmdb in sorted(to_remove)]
    add_payloads = [{"tmdb": tmdb} for tmdb in sorted(to_add)]
    if dry_run:
        log_step(f"Dry-run ativo: seriam adicionados {len(add_payloads)} e removidos {len(remove_payloads)}")
        result["would_add"] = add_payloads
        result["would_remove"] = remove_payloads
        return result
    log_step(f"Aplicando delta: adicionar={len(add_payloads)} remover={len(remove_payloads)}")
    for batch in chunked(add_payloads):
        log_step(f"Enviando lote de adicao com {len(batch)} filmes")
        for movie in batch:
            print(f"Adding movie to Recommended for Jedi: TMDB {movie['tmdb']}")
        result["added_batches"].append(client.post(f"/lists/{list_id}/items/add", {"movies": batch}))
    for batch in chunked(remove_payloads):
        log_step(f"Enviando lote de remocao com {len(batch)} filmes")
        result["removed_batches"].append(client.post(f"/lists/{list_id}/items/remove", {"movies": batch}))
    log_step("Aplicacao de delta concluida")
    return result


def public_movie_row(row: dict[str, Any]) -> dict[str, Any]:
    movie = row["movie"]
    value, votes = imdb_rating(movie)
    return {
        "title": movie.get("title"),
        "year": movie.get("year"),
        "ids": normalize_ids(movie).__dict__,
        "score": row["score"],
        "imdb_value": value,
        "imdb_votes": votes,
        "runtime": movie.get("runtime"),
        "genres": movie_genres(movie),
        "country": movie.get("country"),
        "certification": movie.get("certification"),
        "age_rating": movie.get("age_rating"),
        "matched_keywords": row["score_detail"]["matched_keywords"][:30],
        "matched_genres": row["score_detail"]["matched_genres"],
        "score_components": row["score_detail"]["components"],
        "sources": row.get("sources") or [],
    }


def write_report(report: dict[str, Any], report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"recommended_for_jedi_{dt.date.today().isoformat()}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return path


def _csv_list(value: str) -> list[str]:
    """Parse a comma-separated string into a trimmed list (empty string -> [])."""
    if not value or not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update MDBList Recommended for Jedi.")
    parser.add_argument("--api-key", default=os.getenv("MDBLIST_API_KEY"), help="MDBList API key.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report without writing to MDBList.")
    parser.add_argument("--seed", default=today_seed(), help="Daily variation seed. Defaults to today's date.")
    parser.add_argument("--cache-file", type=Path, default=CACHE_FILE)
    parser.add_argument("--report-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--max-raw-candidates", type=int, default=RAW_CANDIDATE_LIMIT)
    parser.add_argument("--max-enrich", type=int, default=ENRICH_LIMIT)
    parser.add_argument("--sleep", type=float, default=0.05, help="Small delay between API requests.")

    # -- Fine-Tuning arguments -----------------------------------------------
    ft_group = parser.add_argument_group("Fine-Tuning", "Optional filters to refine recommendations.")
    ft_group.add_argument(
        "--exclude-genres",
        type=_csv_list,
        default=[],
        help="Comma-separated list of genres to exclude (e.g. 'Horror,Romance').",
    )
    ft_group.add_argument(
        "--exclude-keywords",
        type=_csv_list,
        default=[],
        help="Comma-separated list of keywords to exclude (e.g. 'zombie,vampire').",
    )
    ft_group.add_argument(
        "--exclude-actors",
        type=_csv_list,
        default=[],
        help="Comma-separated list of actor names to exclude (e.g. 'Adam Sandler,Steven Seagal').",
    )
    ft_group.add_argument(
        "--imdb-min",
        type=float,
        default=None,
        help=f"Minimum IMDB rating (default: {MIN_IMDB_VALUE}).",
    )
    ft_group.add_argument(
        "--imdb-max",
        type=float,
        default=None,
        help="Maximum IMDB rating (default: no limit).",
    )
    ft_group.add_argument(
        "--imdb-min-votes",
        type=int,
        default=None,
        help=f"Minimum IMDB vote count (default: {MIN_IMDB_VOTES}).",
    )
    ft_group.add_argument(
        "--year-min",
        type=int,
        default=None,
        help="Minimum release year (default: no limit).",
    )
    ft_group.add_argument(
        "--year-max",
        type=int,
        default=None,
        help="Maximum release year (default: no limit).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if not args.api_key:
        print("Set MDBLIST_API_KEY or pass --api-key.", file=sys.stderr)
        return 2

    # Build FineTuning from CLI args
    fine_tuning = FineTuning(
        excluded_genres=args.exclude_genres,
        excluded_keywords=args.exclude_keywords,
        excluded_actors=args.exclude_actors,
        imdb_min=args.imdb_min,
        imdb_max=args.imdb_max,
        imdb_min_votes=args.imdb_min_votes,
        year_min=args.year_min,
        year_max=args.year_max,
    )

    log_step(f"Inicio da execucao (seed={args.seed}, dry_run={args.dry_run})")
    log_step("=== Fine-Tuning ===")
    log_step(f"  Generos excluidos:  {fine_tuning.excluded_genres or '(nenhum)'}")
    log_step(f"  Keywords excluidas: {fine_tuning.excluded_keywords or '(nenhum)'}")
    log_step(f"  Atores excluidos:   {fine_tuning.excluded_actors or '(nenhum)'}")
    log_step(f"  IMDB min:           {fine_tuning.effective_imdb_min}")
    log_step(f"  IMDB max:           {fine_tuning.effective_imdb_max or '(sem limite)'}")
    log_step(f"  IMDB votos min:     {fine_tuning.effective_imdb_min_votes}")
    log_step(f"  Ano min:            {fine_tuning.year_min or '(sem limite)'}")
    log_step(f"  Ano max:            {fine_tuning.year_max or '(sem limite)'}")
    log_step("===================")
    client = MDBListClient(args.api_key, sleep=args.sleep)
    cache = MovieCache(args.cache_file)
    report: dict[str, Any] = {
        "generated_at": now_utc(),
        "seed": args.seed,
        "dry_run": args.dry_run,
        "fine_tuning": fine_tuning.to_dict(),
        "errors": [],
    }

    try:
        log_step("Carregando lista Last Watched")
        watched_items = client.list_items(
            f"/lists/{USERNAME}/{LAST_WATCHED_SLUG}/items/movie",
            extra_params={"sort": "watched", "order": "asc"},
        )
        if len(watched_items) < PROFILE_SIZE:
            raise MDBListError(f"Last Watched returned only {len(watched_items)} items.")
        log_step(f"Last Watched carregada com {len(watched_items)} itens")

        watched_identities: set[str] = set()
        for item in watched_items:
            watched_identities.update(movie_identity_set(item))

        blocked_identities: set[str] = set()
        blocked_list_counts: dict[str, int] = {}
        log_step("Carregando listas bloqueadas")
        for slug in sorted(BLOCKED_SOURCE_LIST_SLUGS):
            try:
                blocked_items = client.list_items(f"/lists/{USERNAME}/{slug}/items/movie")
            except MDBListError as exc:
                raise MDBListError(f"Failed to load blocked list '{slug}': {exc}") from exc
            identities_before = len(blocked_identities)
            for item in blocked_items:
                blocked_identities.update(movie_identity_set(item))
            blocked_list_counts[slug] = len(blocked_identities) - identities_before
            log_step(f"Lista bloqueada '{slug}' carregada ({blocked_list_counts[slug]} identidades)")

        excluded_identities = watched_identities | blocked_identities
        log_step(f"Total de identidades excluidas: {len(excluded_identities)}")

        recent_items = watched_items[:PROFILE_SIZE]
        log_step(f"Carregando detalhes dos {len(recent_items)} filmes do perfil")
        recent_movies = [fetch_movie_details(client, cache, normalize_ids(item).tmdb) for item in recent_items]
        recent_preview = [movie.get("title") for movie in recent_movies[:3]]
        log_step(f"Perfil atual (3 mais recentes): {recent_preview}")

        log_step("Construindo perfil de gosto")
        profile = build_profile(recent_movies, args.seed)
        log_step("Construindo pool de candidatos")
        candidates = build_candidate_pool(client, profile, excluded_identities, args, fine_tuning)
        log_step("Buscando lista alvo atual")
        target_list = find_target_list(client)
        if target_list is None:
            current_items: list[dict[str, Any]] = []
            log_step("Lista alvo ainda nao existe; sera criada se necessario")
        else:
            current_items = client.list_items(f"/lists/{target_list['id']}/items", extra_params={"unified": "false"})
            log_step(f"Lista alvo atual tem {len(current_items)} itens")

        ranked, ranking_stats = rank_candidates(
            client,
            cache,
            candidates,
            current_items,
            profile,
            excluded_identities,
            args,
            fine_tuning,
        )
        if len(ranked) < TARGET_SIZE:
            raise MDBListError(f"Only {len(ranked)} valid candidates; refusing to update.")

        log_step("Selecionando alvo final com regras de estabilidade")
        target, target_summary = choose_target(ranked, current_items, excluded_identities)
        if len(target) < TARGET_SIZE:
            raise MDBListError(f"Only {len(target)} target movies after stability rules; refusing to update.")

        if target_list is None:
            log_step("Garantindo criacao da lista alvo")
            target_list = ensure_target_list(client, args.dry_run)

        current_tmdb = {normalize_ids(item).tmdb for item in current_items if normalize_ids(item).tmdb is not None}
        target_tmdb = {normalize_ids(row["movie"]).tmdb for row in target if normalize_ids(row["movie"]).tmdb is not None}
        to_add = sorted(target_tmdb - current_tmdb)
        to_remove = sorted(current_tmdb - target_tmdb)
        log_step(f"Delta calculado: adicionar={len(to_add)} remover={len(to_remove)}")

        write_result = None
        if target_list.get("id") is not None:
            write_result = apply_delta(client, int(target_list["id"]), to_add, to_remove, args.dry_run)

        report.update(
            {
                "source_list": {
                    "username": USERNAME,
                    "slug": LAST_WATCHED_SLUG,
                    "watched_count": len(watched_items),
                },
                "blocked_lists": {
                    "username": USERNAME,
                    "slugs": sorted(BLOCKED_SOURCE_LIST_SLUGS),
                    "counts": blocked_list_counts,
                    "excluded_identities": len(blocked_identities),
                },
                "target_list": target_list,
                "profile": profile,
                "candidate_stats": {
                    "raw_candidates": len(candidates),
                    **ranking_stats,
                    **target_summary,
                },
                "delta": {
                    "to_add": to_add,
                    "to_remove": to_remove,
                    "kept": sorted(current_tmdb & target_tmdb),
                },
                "write_result": write_result,
                "recommendations": [public_movie_row(row) for row in target],
            }
        )
        log_step("Salvando cache e relatorio")
        cache.save()
        report_path = write_report(report, args.report_dir)
        log_step("Processamento concluido com sucesso")
        print(f"Recommended for Jedi {'dry-run ' if args.dry_run else ''}complete.")
        print(f"Watched: {len(watched_items)} | Candidates: {len(candidates)} | Valid ranked: {len(ranked)}")
        print(f"Add: {len(to_add)} | Remove: {len(to_remove)} | Keep: {len(current_tmdb & target_tmdb)}")
        print(f"Report: {report_path}")
        return 0
    except Exception as exc:
        report["errors"].append(str(exc))
        log_step("Erro durante processamento; salvando estado")
        cache.save()
        report_path = write_report(report, args.report_dir)
        print(f"Refusing to update Recommended for Jedi: {exc}", file=sys.stderr)
        print(f"Report: {report_path}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

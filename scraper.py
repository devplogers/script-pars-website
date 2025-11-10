#!/usr/bin/env python3
"""Site-wide web scraping utility.

This script crawls an entire website, extracts page metadata and main text, and
persists the results to a SQLite database. It is intended as a flexible
foundation that can be customized for different projects.
"""
from __future__ import annotations

import argparse
import collections
import contextlib
import logging
import sqlite3
import sys
import textwrap
import time
from datetime import datetime
from typing import Iterable, Optional, Set
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger("scraper")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SiteCrawler/1.0; +https://example.com/bot)"
    )
}


class Database:
    """Lightweight wrapper around a SQLite database."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._create_schema()

    def _create_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                status_code INTEGER,
                content_type TEXT,
                title TEXT,
                text_content TEXT,
                fetched_at TEXT NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_url TEXT NOT NULL,
                to_url TEXT NOT NULL,
                FOREIGN KEY(from_url) REFERENCES pages(url),
                FOREIGN KEY(to_url) REFERENCES pages(url)
            );
            """
        )
        self.conn.commit()

    def upsert_page(
        self,
        url: str,
        status_code: Optional[int],
        content_type: Optional[str],
        title: Optional[str],
        text_content: Optional[str],
    ) -> None:
        fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pages(url, status_code, content_type, title, text_content, fetched_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    status_code=excluded.status_code,
                    content_type=excluded.content_type,
                    title=excluded.title,
                    text_content=excluded.text_content,
                    fetched_at=excluded.fetched_at;
                """,
                (url, status_code, content_type, title, text_content, fetched_at),
            )

    def insert_links(self, from_url: str, to_urls: Iterable[str]) -> None:
        with self.conn:
            self.conn.executemany(
                "INSERT INTO links(from_url, to_url) VALUES(?, ?);",
                ((from_url, url) for url in to_urls),
            )

    def close(self) -> None:
        self.conn.close()


class SiteCrawler:
    def __init__(
        self,
        base_url: str,
        db: Database,
        max_pages: int,
        delay: float,
        timeout: float,
        include_subdomains: bool,
        respect_robots: bool,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.parsed_base = urlparse(self.base_url)
        self.db = db
        self.max_pages = max_pages
        self.delay = delay
        self.timeout = timeout
        self.include_subdomains = include_subdomains
        self.headers = headers or DEFAULT_HEADERS
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.robot_parser: Optional[RobotFileParser] = None
        if respect_robots:
            self.robot_parser = RobotFileParser()
            robots_url = urljoin(self.base_url, "/robots.txt")
            with contextlib.suppress(requests.RequestException):
                response = self.session.get(robots_url, timeout=self.timeout)
                if response.ok:
                    self.robot_parser.parse(response.text.splitlines())

    def allowed_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if parsed.netloc == "":
            return False
        if self.include_subdomains:
            if not parsed.netloc.endswith(self.parsed_base.netloc):
                return False
        else:
            if parsed.netloc != self.parsed_base.netloc:
                return False
        if self.robot_parser and not self.robot_parser.can_fetch(
            self.session.headers.get("User-Agent", "*"), url
        ):
            LOGGER.info("Blocked by robots.txt: %s", url)
            return False
        return True

    def crawl(self) -> None:
        queue: collections.deque[str] = collections.deque([self.base_url])
        visited: Set[str] = set()

        while queue and len(visited) < self.max_pages:
            url = queue.popleft()
            url = urldefrag(url).url  # remove fragments
            if url in visited:
                continue
            if not self.allowed_url(url):
                continue
            visited.add(url)

            LOGGER.info("Fetching %s", url)
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                LOGGER.warning("Failed to fetch %s: %s", url, exc)
                self.db.upsert_page(url, None, None, None, None)
                continue

            content_type = response.headers.get("Content-Type", "").split(";")[0]
            title = None
            text_content = None
            links_to_store: set[str] = set()

            if "text/html" in content_type:
                soup = BeautifulSoup(response.text, "html.parser")
                title = soup.title.string.strip() if soup.title and soup.title.string else None
                text_content = self._extract_text(soup)
                for link in soup.find_all("a", href=True):
                    next_url = urljoin(url, link["href"])
                    next_url = urldefrag(next_url).url
                    if self.allowed_url(next_url) and next_url not in visited:
                        queue.append(next_url)
                        links_to_store.add(next_url)
            else:
                LOGGER.debug("Skipping non-HTML content: %s (%s)", url, content_type)

            self.db.upsert_page(url, response.status_code, content_type, title, text_content)
            if links_to_store:
                self.db.insert_links(url, links_to_store)

            if queue and self.delay:
                time.sleep(self.delay)

    @staticmethod
    def _extract_text(soup: BeautifulSoup) -> str:
        for element in soup(["script", "style", "noscript"]):
            element.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return textwrap.shorten(text, width=10000, placeholder=" …")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl a website and store page data in a SQLite database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("url", help="Base URL of the website to crawl.")
    parser.add_argument(
        "--db-path",
        default="scraper.db",
        help="Path to the SQLite database file.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1000,
        help="Maximum number of pages to crawl.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between requests to avoid overloading the server.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Request timeout in seconds.",
    )
    parser.add_argument(
        "--include-subdomains",
        action="store_true",
        help="Crawl subdomains of the target site.",
    )
    parser.add_argument(
        "--respect-robots",
        action="store_true",
        help="Obey directives from robots.txt.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging output.",
    )
    return parser.parse_args(argv)


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    LOGGER.info("Starting crawl for %s", args.url)
    db = Database(args.db_path)
    crawler = SiteCrawler(
        base_url=args.url,
        db=db,
        max_pages=args.max_pages,
        delay=args.delay,
        timeout=args.timeout,
        include_subdomains=args.include_subdomains,
        respect_robots=args.respect_robots,
    )
    try:
        crawler.crawl()
    finally:
        db.close()
    LOGGER.info("Crawl finished.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

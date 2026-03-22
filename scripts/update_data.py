#!/usr/bin/env python3
import csv
import datetime
import io
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def now_utc_str():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_json_load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def urlopen_text(url, timeout=20, retries=3, backoff=1.5, headers=None):
    headers = {"User-Agent": "Mozilla/5.0", **(headers or {})}
    last_error = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    raise last_error


def fetch_csv(symbol):
    url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
    text = urlopen_text(url, timeout=20, retries=3)
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return None
    return rows[0]


def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").replace("\xa0", " ").strip()


def translate_ko(text):
    if not text:
        return ""
    try:
        q = urllib.parse.quote(text)
        url = f"https://api.mymemory.translated.net/get?q={q}&langpair=en|ko"
        data = json.loads(urlopen_text(url, timeout=20, retries=2))
        return (data.get("responseData", {}) or {}).get("translatedText") or ""
    except Exception:
        return ""


def fetch_rss_items(url, limit=6):
    xml = urlopen_text(url, timeout=20, retries=2)
    items = []
    for part in xml.split("<item>")[1:]:
        try:
            title = strip_html(part.split("<title>")[1].split("</title>")[0])
            link = strip_html(part.split("<link>")[1].split("</link>")[0])
            desc = ""
            if "<description>" in part:
                desc = strip_html(part.split("<description>")[1].split("</description>")[0])
            items.append({"title": title, "link": link, "summary": desc[:220]})
            if len(items) >= limit:
                break
        except Exception:
            continue
    return items


def with_fallback(fetcher, fallback_value=None):
    try:
        return fetcher()
    except Exception:
        return fallback_value


def main():
    existing_market = safe_json_load(DATA_DIR / "market.json", {})
    existing_news = safe_json_load(DATA_DIR / "news.json", {"items": []})

    market = {
        "updated_at_utc": now_utc_str(),
        "indices": {
            "spx": with_fallback(lambda: fetch_csv("^spx"), existing_market.get("indices", {}).get("spx")),
            "ndq": with_fallback(lambda: fetch_csv("^ndq"), existing_market.get("indices", {}).get("ndq")),
            "dji": with_fallback(lambda: fetch_csv("^dji"), existing_market.get("indices", {}).get("dji")),
        },
        "futures": {
            "es": with_fallback(lambda: fetch_csv("es.f"), existing_market.get("futures", {}).get("es")),
            "nq": with_fallback(lambda: fetch_csv("nq.f"), existing_market.get("futures", {}).get("nq")),
            "ym": with_fallback(lambda: fetch_csv("ym.f"), existing_market.get("futures", {}).get("ym")),
        },
        "fx": {
            "usdkrw": existing_market.get("fx", {}).get("usdkrw")
        }
    }

    fx_data = with_fallback(
        lambda: json.loads(urlopen_text("https://open.er-api.com/v6/latest/USD", timeout=20, retries=3))
    )
    if fx_data:
        market["fx"]["usdkrw"] = {
            "rate": fx_data.get("rates", {}).get("KRW"),
            "time_last_update_utc": fx_data.get("time_last_update_utc")
        }

    news = {
        "updated_at_utc": market["updated_at_utc"],
        "items": []
    }

    sources = [
        "https://www.investing.com/rss/news_25.rss",
        "https://www.investing.com/rss/news_95.rss",
        "https://www.fxstreet.com/rss/news",
    ]
    for src in sources:
        items = with_fallback(lambda s=src: fetch_rss_items(s, limit=4), [])
        news["items"].extend(items)

    seen = set()
    deduped = []
    for it in news["items"]:
        if not it.get("title") or it["title"] in seen:
            continue
        seen.add(it["title"])
        deduped.append(it)
    news["items"] = deduped[:10]

    if not news["items"]:
        news = existing_news or news
        news["updated_at_utc"] = market["updated_at_utc"]
    else:
        for it in news["items"]:
            it["title_ko"] = translate_ko(it.get("title", ""))
            it["summary_ko"] = translate_ko(it.get("summary", ""))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / "market.json", "w", encoding="utf-8") as f:
        json.dump(market, f, ensure_ascii=False, indent=2)
    with open(DATA_DIR / "news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

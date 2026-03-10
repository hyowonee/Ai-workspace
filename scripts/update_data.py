#!/usr/bin/env python3
import csv, io, json, urllib.request, datetime, re


def fetch_csv(symbol):
    url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
    with urllib.request.urlopen(url, timeout=15) as r:
        text = r.read().decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return None
    return rows[0]


def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").replace("\xa0", " ").strip()


def translate_ko(text):
    if not text:
        return ""
    payload = json.dumps({
        "q": text,
        "source": "auto",
        "target": "ko",
        "format": "text"
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://libretranslate.de/translate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
        return data.get("translatedText") or ""
    except Exception:
        return ""


def fetch_rss_items(url, limit=6):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        xml = r.read().decode("utf-8")
    items = []
    for part in xml.split("<item>")[1:]:
        title = strip_html(part.split("<title>")[1].split("</title>")[0])
        link = strip_html(part.split("<link>")[1].split("</link>")[0])
        desc = ""
        if "<description>" in part:
            desc = strip_html(part.split("<description>")[1].split("</description>")[0])
        items.append({"title": title, "link": link, "summary": desc[:220]})
        if len(items) >= limit:
            break
    return items


def main():
    market = {
        "updated_at_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "indices": {
            "spx": fetch_csv("^spx"),
            "ndq": fetch_csv("^ndq"),
            "dji": fetch_csv("^dji"),
        },
        "futures": {
            "es": fetch_csv("es.f"),
            "nq": fetch_csv("nq.f"),
            "ym": fetch_csv("ym.f"),
        },
        "fx": {
            "usdkrw": None
        }
    }

    # USD/KRW from exchangerate-api (free)
    try:
        with urllib.request.urlopen("https://open.er-api.com/v6/latest/USD", timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        market["fx"]["usdkrw"] = {
            "rate": data.get("rates", {}).get("KRW"),
            "time_last_update_utc": data.get("time_last_update_utc")
        }
    except Exception:
        pass

    news = {
        "updated_at_utc": market["updated_at_utc"],
        "items": []
    }

    # News sources (free RSS)
    sources = [
        "https://www.investing.com/rss/news_25.rss",
        "https://www.investing.com/rss/news_95.rss",
        "https://www.fxstreet.com/rss/news",
    ]
    for src in sources:
        try:
            news["items"].extend(fetch_rss_items(src, limit=4))
        except Exception:
            continue

    # Deduplicate by title
    seen = set()
    deduped = []
    for it in news["items"]:
        if it["title"] in seen:
            continue
        seen.add(it["title"])
        deduped.append(it)
    news["items"] = deduped[:10]

    # Translate to Korean (best effort)
    for it in news["items"]:
        it["title_ko"] = translate_ko(it["title"])
        it["summary_ko"] = translate_ko(it["summary"])

    with open("data/market.json", "w", encoding="utf-8") as f:
        json.dump(market, f, ensure_ascii=False, indent=2)
    with open("data/news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()

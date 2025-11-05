# tabelog_scrape_all.py
# -*- coding: utf-8 -*-
"""
食べログの検索一覧ページから全店舗の詳細URLを収集し、
各詳細ページから「店舗名」「住所」「電話番号」「HP」を取得してCSVに保存します。

使い方:
  python tabelog_scrape_all.py https://tabelog.com/osaka/A2701/A270108/rstLst/  output.csv

注意:
- 必ず対象サイトの利用規約・robots.txtを確認し、過度なアクセスを避けてください。
- 一覧URLは都道府県・エリアの一覧ページ(rstLst)を指定してください。
  例: https://tabelog.com/osaka/A2701/A270108/rstLst/
"""

import sys
import re
import time
import csv
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "Connection": "keep-alive",
}

REQ_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 1.5
REQUEST_INTERVAL = 1.0  # レート制限（秒）

class ScrapeError(Exception):
    pass

def fetch_html(url: str) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
            if resp.status_code != 200:
                raise ScrapeError(f"HTTP {resp.status_code}: {url}")
            resp.encoding = resp.apparent_encoding
            return resp.text
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP)
    raise ScrapeError(f"Failed to fetch {url}: {last_err}")

def absolutize(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http"):
        return href
    # 食べログは絶対URLを返すことが多いが、念のため
    if href.startswith("/"):
        # ベースのスキームとホストを推定
        m = re.match(r"^(https?://[^/]+)/", base)
        if m:
            return m.group(1) + href
    return None

def parse_list_page_for_detail_urls(html: str, base_url: str) -> Tuple[List[str], Optional[str]]:
    """
    一覧ページから店舗詳細URL群と「次へ」ページURLを抽出
    """
    soup = BeautifulSoup(html, "html.parser")
    detail_urls: List[str] = []

    # 店舗カード内のリンク: a.rstname or a.list-rst__rst-name-target など
    for a in soup.select("a.list-rst__rst-name-target, a.rstname, a.js-clickable-area"):
        href = a.get("href")
        if not href:
            continue
        absu = absolutize(base_url, href)
        if not absu:
            continue
        # 詳細ページURLのパターン: https://tabelog.com/XXX/XXXX/XXXXXXX/
        if re.search(r"^https?://tabelog\.com/.+/\d{6,}/?$", absu):
            detail_urls.append(absu)

    # フォールバック: 店名リンク以外でも rstinfo への導線がある場合
    if not detail_urls:
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if re.search(r"^https?://tabelog\.com/.+/\d{6,}/?$", href):
                detail_urls.append(href)

    # 「次へ」ページ
    next_url = None
    # 代表的なページネーション: a.next, a.pagination__next
    next_candidates = soup.select("a.next, a.pagination__next, a.c-pagination__arrow--next")
    for a in next_candidates:
        href = a.get("href")
        absu = absolutize(base_url, href) if href else None
        if absu:
            next_url = absu
            break

    # 別のパターン: ページ番号リンクで現在ページの次の番号
    if not next_url:
        current_page_num = None
        # 現在ページは span.curr, li.is-current など
        curr = soup.select_one("span.curr, li.is-current")
        if curr:
            try:
                current_page_num = int(curr.get_text(strip=True))
            except:
                current_page_num = None
        if current_page_num:
            for a in soup.select("a[href]"):
                txt = a.get_text(strip=True)
                try:
                    n = int(txt)
                except:
                    continue
                if n == current_page_num + 1:
                    absu = absolutize(base_url, a.get("href") or "")
                    if absu:
                        next_url = absu
                        break

    # ユニーク化
    detail_urls = list(dict.fromkeys(detail_urls))
    return detail_urls, next_url

def text_or_none(el) -> Optional[str]:
    if not el:
        return None
    txt = el.get_text(strip=True)
    return txt or None

def extract_store_info(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    # 店舗名
    name = None
    for sel in ["h1", "h2"]:
        n = text_or_none(soup.select_one(sel))
        if n:
            name = re.sub(r"\s*-\s*.*$", "", n)
            break
    if not name:
        # テーブルの「店名」
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td and "店名" in th.get_text(strip=True):
                    name = text_or_none(td)
                    break
            if name:
                break

    # 住所
    address = None
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td and "住所" in th.get_text(strip=True):
                parts = [s for s in td.stripped_strings]
                addr = " ".join(parts)
                addr = re.sub(r"(地図|大きな地図を見る|周辺のお店を探す).*", "", addr)
                address = addr.strip()
                break
        if address:
            break
    if not address:
        p = soup.select_one("p.rstinfo-table__address")
        address = text_or_none(p)

    # 電話番号（「予約・お問い合わせ」「電話番号」など）
    phone = None
    phone_labels = ["電話番号", "予約・お問い合わせ", "電話受付"]
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td and any(lbl in th.get_text(strip=True) for lbl in phone_labels):
                txt = td.get_text(" ", strip=True)
                m = re.search(r"\b0\d{1,4}-\d{1,4}-\d{3,4}\b", txt)
                if m:
                    phone = m.group(0)
                    break
                tel_a = td.find("a", href=re.compile(r"^tel:"))
                if tel_a:
                    phone = tel_a.get_text(strip=True)
                    break
        if phone:
            break
    if not phone:
        m = re.search(r"\b0\d{1,4}-\d{1,4}-\d{3,4}\b", soup.get_text(" ", strip=True))
        if m:
            phone = m.group(0)

    # HP
    hp = None
    hp_labels = ["HP", "ホームページ", "オフィシャルサイト", "公式サイト", "公式ホームページ"]
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td and any(lbl in th.get_text(strip=True) for lbl in hp_labels):
                a = td.find("a", href=True)
                if a and a["href"].startswith("http"):
                    hp = a["href"]
                    break
        if hp:
            break
    if not hp:
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"]
            if any(lbl in text for lbl in hp_labels) and href.startswith("http"):
                hp = href
                break

    return {
        "店舗名": name,
        "住所": address,
        "電話番号": phone,
        "HP": hp,
    }

def crawl_all_details(list_url: str) -> List[str]:
    """
    一覧ページのページネーションを辿って、全詳細URLを収集
    """
    all_urls: List[str] = []
    seen_pages = set()
    next_url = list_url
    while next_url and next_url not in seen_pages:
        seen_pages.add(next_url)
        time.sleep(REQUEST_INTERVAL)
        html = fetch_html(next_url)
        detail_urls, nxt = parse_list_page_for_detail_urls(html, next_url)
        # 追加
        for u in detail_urls:
            if u not in all_urls:
                all_urls.append(u)
        next_url = nxt
    return all_urls

def main():
    if len(sys.argv) < 3:
        print("使い方: python tabelog_scrape_all.py <一覧URL(rstLst)> <出力CSV>")
        sys.exit(1)
    list_url = sys.argv[1].strip()
    out_csv = sys.argv[2].strip()

    # 詳細URL収集
    print(f"[INFO] 一覧URLから詳細URLを収集: {list_url}")
    detail_urls = crawl_all_details(list_url)
    print(f"[INFO] 収集件数: {len(detail_urls)}")

    # 各詳細をスクレイプ
    rows: List[Dict[str, Optional[str]]] = []
    for i, url in enumerate(detail_urls, 1):
        try:
            time.sleep(REQUEST_INTERVAL)
            html = fetch_html(url)
            info = extract_store_info(html)
            info["詳細URL"] = url
            rows.append(info)
            print(f"[{i}/{len(detail_urls)}] OK: {info.get('店舗名') or ''} ({url})")
        except Exception as e:
            print(f"[{i}/{len(detail_urls)}] ERROR: {url} -> {e}")

    # CSV保存
    fieldnames = ["店舗名", "住所", "電話番号", "HP", "詳細URL"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[INFO] 書き出し完了: {out_csv}")

if __name__ == "__main__":
    main()


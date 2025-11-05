# scrape_tabelog_store.py
# -*- coding: utf-8 -*-
"""
Tabelog 店舗ページから「店舗名」「住所」「電話番号」「HP」を取得するスクレイパー
- requests + BeautifulSoup4
- 丁寧なヘッダ、簡易リトライ、セレクタのフォールバックを実装
- 注意: スクレイピングは必ず対象サイトの利用規約・robots.txtを確認し、過度なアクセスを避けてください
"""

import re
import time
from typing import Optional, Dict
import requests
from bs4 import BeautifulSoup


class TabelogScraperError(Exception):
    pass


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
    "Connection": "keep-alive",
}


def fetch_html(
    url: str, timeout: float = 20.0, max_retries: int = 3, sleep_sec: float = 1.5
) -> str:
    last_err: Optional[Exception] = None
    session = requests.Session()
    for attempt in range(max_retries):
        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            # 一部ページは 403 対策として Accept-Language / UA を強めに設定済み
            if resp.status_code != 200:
                raise TabelogScraperError(f"HTTP {resp.status_code} for {url}")
            # エンコーディング推定
            resp.encoding = resp.apparent_encoding
            return resp.text
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise TabelogScraperError(f"Failed to fetch {url}: {last_err}")


def text_or_none(el) -> Optional[str]:
    if not el:
        return None
    txt = el.get_text(strip=True)
    return txt or None


def extract_store_name(soup: BeautifulSoup) -> Optional[str]:
    # 1) 店名がページの上部タイトルや詳細テーブルにある
    selectors = [
        "h2",  # 店名見出し（例: 店ページの大見出し）
        "h1",  # 場合によって h1
        "div.rstinfo-table__name a",  # 旧構造
        "table tr th:contains('店名') ~ td",  # jQuery擬似は使えないので後で手動探索
    ]
    # まず見出し候補から抽出
    for sel in ["h1", "h2"]:
        el = soup.select_one(sel)
        name = text_or_none(el)
        if name and len(name) > 0:
            # 余計な説明文を含む場合を整形
            name = re.sub(r"\s*-\s*.*$", "", name)  # "店名 - 説明" → 店名
            return name

    # テーブル内「店名」ラベル探索
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                th_text = th.get_text(strip=True)
                if "店名" in th_text:
                    return text_or_none(td)
    return None


def extract_address(soup: BeautifulSoup) -> Optional[str]:
    # 詳細テーブルの「住所」セルを探す
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                th_text = th.get_text(strip=True)
                if "住所" in th_text:
                    # aタグ群や改行が入るケースを整形
                    # 例: 大阪府大阪市福島区福島7-7-8 と地図リンクが同セルにある
                    parts = []
                    # 文字列と <a> のテキストを結合
                    for node in td.stripped_strings:
                        parts.append(node)
                    address = " ".join(parts)
                    # 地図など余計な文言を削る
                    address = re.sub(
                        r"(地図|大きな地図を見る|周辺のお店を探す).*", "", address
                    )
                    address = address.strip()
                    return address

    # 旧構造: p.rstinfo-table__address 等
    el = soup.select_one("p.rstinfo-table__address")
    if el:
        return text_or_none(el)

    return None


def extract_phone(soup: BeautifulSoup) -> Optional[str]:
    # 詳細テーブルの「電話番号」または「予約・お問い合わせ」を探す
    phone_labels = ["電話番号", "予約・お問い合わせ", "電話受付"]
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                th_text = th.get_text(strip=True)
                if any(lbl in th_text for lbl in phone_labels):
                    # 数字・ハイフンのみ抽出（050-xxxx-xxxx など）
                    text = td.get_text(" ", strip=True)
                    m = re.search(r"\b0\d{1,4}-\d{1,4}-\d{3,4}\b", text)
                    if m:
                        return m.group(0)
                    # セル内リンクtel:
                    tel_a = td.find("a", href=re.compile(r"^tel:"))
                    if tel_a:
                        return tel_a.get_text(strip=True)

    # 他の場所に表示されることもある
    # 全文から最初の電話らしき番号を拾うフォールバック
    full_text = soup.get_text(" ", strip=True)
    m = re.search(r"\b0\d{1,4}-\d{1,4}-\d{3,4}\b", full_text)
    if m:
        return m.group(0)

    return None


def extract_homepage_url(soup: BeautifulSoup) -> Optional[str]:
    # 「HP」「ホームページ」「オフィシャルサイト」等のラベルを探索
    hp_labels = [
        "HP",
        "ホームページ",
        "オフィシャルサイト",
        "公式サイト",
        "公式ホームページ",
    ]
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                th_text = th.get_text(strip=True)
                if any(lbl in th_text for lbl in hp_labels):
                    a = td.find("a", href=True)
                    if a and a["href"].startswith("http"):
                        return a["href"]

    # 店舗情報下部に「関連リンク」などでHPが置かれる場合のフォールバック
    for a in soup.find_all("a", href=True):
        link_text = a.get_text(strip=True)
        if any(lbl in link_text for lbl in hp_labels):
            href = a["href"]
            if href.startswith("http"):
                return href

    return None


def scrape_tabelog_store(url: str) -> Dict[str, Optional[str]]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    name = extract_store_name(soup)
    address = extract_address(soup)
    phone = extract_phone(soup)
    hp = extract_homepage_url(soup)
    return {
        "店舗名": name,
        "住所": address,
        "電話番号": phone,
        "HP": hp,
    }


if __name__ == "__main__":
    # 使い方:
    # python scrape_tabelog_store.py
    # 実際の店舗URLを渡して利用してください
    test_url = (
        "https://tabelog.com/akita/A0501/A050101/5000664/"  # 例: 実店舗URLに置き換え
    )
    try:
        data = scrape_tabelog_store(test_url)
        print(data)
    except Exception as e:
        print(f"Error: {e}")

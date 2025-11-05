# scraper.py
# -*- coding: utf-8 -*-
"""
代理店募集サイトなどから会社情報を抽出してCSV出力するスクレイパー
- 入力: all_urls.csv （1列目にURLが入っているCSV）
- 出力: scraped_companies.csv
- 必須カラム: ["取得日時", "取得URL", "名称", "住所"]
- ページごとにHTML構造が異なることを想定し、複数の抽出戦略を試行
- 並列処理で高速化（デフォルト: 同時10リクエスト）
"""

import csv
import datetime
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定
INPUT_CSV = "urls.csv"
OUTPUT_CSV = "scraped_companies.csv"
CONCURRENCY = 10
REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CompanyScraper/1.0; +https://example.com/bot)"
}

# 必須カラムと出力カラム
REQUIRED_COLUMNS = ["取得日時", "取得URL", "名称", "住所"]
# 任意追加カラム（ページから取れれば付与）
EXTRA_COLUMNS = [
    "代表者",
    "設立",
    "資本金",
    "事業内容",
    "所在地",  # 住所と同義で出てくることがある
    "電話番号",
    "メール",
    "募集企業",
    "募集地域",
    "初期費用",
]

# 住所っぽい文字列を見つける正規表現（日本住所に寄せた簡易版）
ADDRESS_PAT = re.compile(r"(〒?\s*\d{3}-\d{4}\s*)?([都道府県].*?(区|市|郡|町|村).*)")

PHONE_PAT = re.compile(r"0\d{1,4}-\d{1,4}-\d{4}")
EMAIL_PAT = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def read_urls(path: str) -> List[str]:
    urls: List[str] = []
    p = Path(path)
    if not p.exists():
        print(f"入力CSVが見つかりません: {path}", file=sys.stderr)
        return urls
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            url = row[0].strip()
            if url and url.startswith(("http://", "https://")):
                urls.append(url)
    return urls


def fetch(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        # 一部サイトで日本語文字化け防止
        if resp.encoding is None or resp.encoding == "ISO-8859-1":
            resp.encoding = resp.apparent_encoding
        if resp.status_code == 200:
            return resp.text
        else:
            return None
    except requests.RequestException:
        return None


def textnorm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def extract_table_kv(soup: BeautifulSoup) -> Dict[str, str]:
    """
    ページ内のテーブルから「見出し: 値」を辞書化する。
    代理店募集サイトに多い「|項目|値|」形式のテーブルに対応。
    """
    kv: Dict[str, str] = {}
    # 一般的なtableを走査
    for table in soup.find_all("table"):
        # ヘッダー行に項目があり、次セルが値のケースと、左ヘッダ・右値のケース両方を見る
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                key = textnorm(cells[0].get_text(" ", strip=True))
                val = textnorm(cells[1].get_text(" ", strip=True))
                if key and val:
                    kv[key] = val
    return kv


def guess_name_from_headings(soup: BeautifulSoup) -> Optional[str]:
    """
    ページの見出しから名称（会社名や募集企業名）を推定。
    """
    # 明示的な「募集企業」「企業名」など
    candidates = []
    for label in ["募集企業", "企業名", "会社名", "運営会社", "事業者名"]:
        node = soup.find(string=re.compile(label))
        if node:
            # 近傍のテキストを拾ってみる
            txt = textnorm(node)
            if ":" in txt:
                part = txt.split(":")[-1].strip()
                if part:
                    candidates.append(part)

    # h1/h2/h3から推定（「募集企業：SMBC GMO PAYMENT株式会社」などのパターン）
    for tag in ["h1", "h2", "h3"]:
        for h in soup.find_all(tag):
            t = textnorm(h.get_text(" ", strip=True))
            # 「募集企業：XXX」「企業名：XXX」
            m = re.search(r"(募集企業|企業名|会社名)\s*[:：]\s*(.+)", t)
            if m:
                candidates.append(textnorm(m.group(2)))
            # 括弧内やタイトルに社名が含まれる場合
            if "株式会社" in t or "有限会社" in t:
                candidates.append(t)

    # テーブルKVに「募集企業」「企業名」があれば優先
    kv = extract_table_kv(soup)
    for key in ["募集企業", "企業名", "会社名"]:
        if key in kv and kv[key]:
            candidates.insert(0, kv[key])

    # 重複削除・意味のある文字列に絞る
    unique = []
    for c in candidates:
        c = textnorm(c)
        if c and c not in unique:
            unique.append(c)

    # 会社名らしいものを優先選択
    for c in unique:
        if "株式会社" in c or "有限会社" in c or "合同会社" in c:
            return c
    # 最後の手段：一番長い候補
    return unique[0] if unique else None


def extract_address(soup: BeautifulSoup, kv: Dict[str, str]) -> Optional[str]:
    """
    住所を抽出。テーブルKVの「所在地」「住所」があればそのまま。
    無ければ本文から郵便番号+都道府県などのパターンを検出。
    """
    for key in ["所在地", "住所"]:
        if key in kv and kv[key]:
            return kv[key]

    # 本文から探索
    # 代表的な住所が書かれやすい要素を優先
    text_blobs: List[str] = []
    for sel in ["p", "li", "div"]:
        for node in soup.find_all(sel):
            t = textnorm(node.get_text(" ", strip=True))
            if t:
                text_blobs.append(t)

    # 住所正規表現で最初にマッチしたものを返す
    for t in text_blobs:
        m = ADDRESS_PAT.search(t)
        if m:
            addr = textnorm(m.group(0))
            return addr

    return None


def extract_extras(soup: BeautifulSoup, kv: Dict[str, str]) -> Dict[str, str]:
    """
    任意カラムを抽出（代表者・設立・資本金・事業内容・電話番号・メールなど）
    """
    out: Dict[str, str] = {}

    # テーブル優先
    for key in EXTRA_COLUMNS:
        if key in kv and kv[key]:
            out[key] = kv[key]

    # 本文から補完
    whole_text = textnorm(soup.get_text(" ", strip=True))

    # 電話番号
    if "電話番号" not in out:
        m = PHONE_PAT.search(whole_text)
        if m:
            out["電話番号"] = m.group(0)

    # メール
    if "メール" not in out:
        m = EMAIL_PAT.search(whole_text)
        if m:
            out["メール"] = m.group(0)

    return out


def parse_page(url: str, html: str) -> Optional[Dict[str, str]]:
    """
    単一ページからレコードを生成。名称が取れない場合は None を返す。
    """
    soup = BeautifulSoup(html, "html.parser")

    kv = extract_table_kv(soup)
    name = guess_name_from_headings(soup)

    # 名称が取れない場合は出力しない
    if not name:
        return None

    # 住所の抽出
    addr = extract_address(soup, kv)

    # 任意項目
    extras = extract_extras(soup, kv)

    # 必須レコード
    record: Dict[str, str] = {
        "取得日時": datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "取得URL": url,
        "名称": name,
        "住所": addr if addr else "",
    }

    # 追加カラムをマージ（存在するもののみ）
    for k, v in extras.items():
        if v:
            record[k] = v

    # 募集地域・初期費用などがテーブルにある場合も補完
    for k in ["募集地域", "初期費用"]:
        if k in kv and kv[k]:
            record[k] = kv[k]

    return record


def process_url(url: str) -> Optional[Dict[str, str]]:
    html = fetch(url)
    if not html:
        return None
    return parse_page(url, html)


def unify_columns(records: List[Dict[str, str]]) -> List[str]:
    """
    出力CSVの列順を決める。
    必須カラムは先頭に固定、任意カラムは出現したものを続ける。
    """
    cols = list(REQUIRED_COLUMNS)
    seen = set(cols)
    for rec in records:
        if not rec:
            continue
        for k in rec.keys():
            if k not in seen:
                seen.add(k)
                cols.append(k)
    return cols


def save_csv(path: str, records: List[Dict[str, str]]) -> None:
    if not records:
        print("出力対象レコードがありません（全ページで名称が取得できませんでした）")
        return
    cols = unify_columns(records)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)
    print(f"書き出し完了: {path}（{len(records)}件）")


def main():
    urls = read_urls(INPUT_CSV)
    if not urls:
        print("all_urls.csv にURLがありません。")
        return

    results: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        future_map = {executor.submit(process_url, url): url for url in urls}
        for fut in as_completed(future_map):
            url = future_map[fut]
            try:
                rec = fut.result()
                if rec and rec.get("名称"):
                    results.append(rec)
            except Exception as e:
                # ページごとの失敗は全体に影響させない
                print(f"処理失敗: {url} - {e}", file=sys.stderr)

    save_csv(OUTPUT_CSV, results)


if __name__ == "__main__":
    main()

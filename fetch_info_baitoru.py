import time
import random
import re
from urllib.parse import urljoin, urlparse
import csv
from datetime import datetime
from typing import Iterable
import requests
from bs4 import BeautifulSoup

BASE_DOMAIN = "www.baitoru.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
BASE_BACKOFF_SEC = 3.0

# /cjlist12345/ のような企業IDを拾うための正規表現
CJLIST_RE = re.compile(r"/cjlist(\d+)/", re.IGNORECASE)

# 「これは求人詳細ページっぽい」というURLを絞るための正規表現。使わないかもしれない。
# ここは実際のHTML構造を見て、自分で調整する必要がある
JOB_DETAIL_RE = re.compile(r"/job[0-9]+|/jobview/|/detail/", re.IGNORECASE)


def fetch_html(url: str, session: requests.Session) -> str | None:
    """
    指定URLからHTML文字列を取得する。
    軽いリトライ付きで、失敗時は None を返す。
    """
    last_exc = None
    for i in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                resp.encoding = resp.apparent_encoding
                return resp.text
            else:
                print(f"[WARN] fetch_html status={resp.status_code} url={url}")
                return None
        except requests.RequestException as e:
            last_exc = e
            print(f"[WARN] fetch_html error={e} url={url} try={i+1}")
            time.sleep(BASE_BACKOFF_SEC * (i + 1))

    print(f"[ERROR] fetch_html failed after retries url={url} exc={last_exc}")
    return None


def extract_cjlist_ids_from_html(html: str) -> set[str]:
    """
    ページ中の a[href] から /cjlist{ID}/ をすべて拾って ID の集合を返す。
    """
    soup = BeautifulSoup(html, "lxml")
    ids: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = CJLIST_RE.search(href)
        if not m:
            # 絶対URLとして include の可能性があるので、一応パス部分でも見る
            try:
                path = urlparse(href).path
            except Exception:
                continue
            m = CJLIST_RE.search(path)

        if m:
            ids.add(m.group(1))

    return ids


def extract_job_links_from_listing_html(html: str, base_url: str) -> set[str]:
    """
    求人一覧ページのHTMLから求人詳細ページのURLを集める。
    今回のバイトル構造では、<ul class="ul01"> 内の
    <li class="li01"><h3><a href="..."> が求人詳細URLを持つので、
    そこを起点にしてURLを抽出する。
    """
    soup = BeautifulSoup(html, "lxml")
    links: set[str] = set()

    # <ul class="ul01"> の中の <li class="li01"> → その中の <h3><a href="...">
    for li in soup.select("ul.ul01 li.li01"):
        a = li.find("a", href=True)
        if not a:
            continue

        href = a["href"]
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)

        # ドメインがズレているリンクを弾く
        if parsed.netloc and parsed.netloc != BASE_DOMAIN:
            continue

                # ここで job詳細だけに絞る
        if not JOB_DETAIL_RE.search(parsed.path):
            continue


        links.add(abs_url)

    return links

def extract_company_url_from_job_html(html: str, job_url: str) -> str | None:
    """
    求人詳細ページのHTMLから
    「この会社の情報をもっと見る」リンクを探し、
    企業情報ページの正規URLを返す。
    見つからなければ None を返す。
    """
    soup = BeautifulSoup(html, "lxml")

    # バイトル側で文言が「この会社についてもっと詳しく」等に変わったら壊れる
    a = soup.find(
        "a",
        string=lambda s: s and "この会社の情報をもっと見る" in s,
        href=True,
    )
    if not a:
        return None

    href = a["href"]
    # /cjlist576894/#comp → 絶対URLに変換
    abs_url = urljoin(job_url, href)

    # アンカーを削除してパス部分だけ取り出す
    parsed = urlparse(abs_url)
    company_path = parsed.path  # 例: "/cjlist576894/"

    # 正規化した企業URLを構成する
    company_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", company_path)
    return company_url


def find_next_page_url(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")

    def is_valid_href(href: str) -> bool:
        if not href:
            return False
        # javascript: で始まるものは除外
        if href.strip().lower().startswith("javascript:"):
            return False
        return True

    # 1. rel="next" を優先
    a = soup.find("a", rel="next")
    if a and is_valid_href(a.get("href", "")):
        return urljoin(base_url, a["href"])

    # 2. テキストで候補を探す
    candidates = ["次へ", "次のページ", "次>", "Next", "›"]
    for text in candidates:
        a = soup.find("a", string=lambda s: s and text in s)
        if a and is_valid_href(a.get("href", "")):
            return urljoin(base_url, a["href"])

    return None



def crawl_baitoru_company_ids(
    start_listing_url: str,
    max_listing_pages: int = 100,
    max_job_pages: int = 100000,
    sleep_min: float = 1.0,
    sleep_max: float = 3.0,
) -> list[dict]:
    """
    会社ごとに1行:
    {
        "company_id": "25189",
        "company_url": "https://www.baitoru.com/cjlist25189/",
        "source_job_url": "https://www.baitoru.com/kanto/....../job150291933/",
        "source_listing_url": "https://www.baitoru.com/kanto/jlist/",
    }
    """

    session = requests.Session()

    company_infos: dict[str, dict] = {}       # ★ 本体：company_id -> info dict
    visited_listing_urls: set[str] = set()
    visited_job_urls: set[str] = set()

    # 一覧ページ側
    listing_url = start_listing_url
    listing_count = 0

    while listing_url and listing_count < max_listing_pages:
        current_listing_url = listing_url
        if current_listing_url in visited_listing_urls:
            print(f"[INFO] already visited listing: {current_listing_url}")
            break
        visited_listing_urls.add(current_listing_url)
        listing_count += 1

        print(f"[INFO] listing page {listing_count}: {current_listing_url}")
        html = fetch_html(current_listing_url, session)
        if not html:
            break

        # 一覧ページ中の cjlist を直接拾う
        ids_on_listing = extract_cjlist_ids_from_html(html)
        new_ids = [cid for cid in ids_on_listing if cid not in company_infos]
        if new_ids:
            print(f"[INFO]  listing: found {len(new_ids)} new company IDs")
        for cid in new_ids:
            company_infos[cid] = {
                "company_id": cid,
                "company_url": f"https://{BASE_DOMAIN}/cjlist{cid}/",
                "source_job_url": "",
                "source_listing_url": current_listing_url,
            }

        # 求人詳細リンクを拾う
        job_links = extract_job_links_from_listing_html(html, current_listing_url)
        print(f"[INFO]  listing: found {len(job_links)} job links")

        # 次ページを探す
        next_url = find_next_page_url(html, current_listing_url)

        # 求人詳細ページ側もある程度の数までクロールする
        for job_url in job_links:
            if len(visited_job_urls) >= max_job_pages:
                print("[WARN] reached max_job_pages limit")
                break
            if job_url in visited_job_urls:
                continue

            visited_job_urls.add(job_url)
            print(f"[INFO]  job page: {job_url}")
            job_html = fetch_html(job_url, session)
            if not job_html:
                continue

            ids_on_job = extract_cjlist_ids_from_html(job_html)
            new_ids_job = [cid for cid in ids_on_job if cid not in company_infos]
            if new_ids_job:
                print(
                    f"[INFO]   job: found {len(new_ids_job)} new company IDs "
                    f"from {job_url}"
                )
                for cid in new_ids_job:
                    company_infos[cid] = {
                        "company_id": cid,
                        "company_url": f"https://{BASE_DOMAIN}/cjlist{cid}/",
                        "source_job_url": job_url,
                        "source_listing_url": current_listing_url,
                    }

            company_url = extract_company_url_from_job_html(job_html, job_url)

            if company_url:
                path = urlparse(company_url).path  # "/cjlist576894/"
                m = CJLIST_RE.search(path)
                if m:
                    cid = m.group(1)

                    # 初めて見る company_id なら info を登録
                    info = company_infos.setdefault(
                        cid,
                        {
                            "company_id": cid,
                            "company_url": company_url,
                            "source_job_url": job_url,
                            "source_listing_url": current_listing_url,
                        },
                    )
                    info["company_url"] = company_url
                    if not info.get("source_job_url"):
                        info["source_job_url"] = job_url
                    if not info.get("source_listing_url"):
                        info["source_listing_url"] = current_listing_url
            
            # サーバ負荷対策
            time.sleep(random.uniform(sleep_min, sleep_max))

        # Job URL をキューに積む前/次ページに進む前に少し待つ
        time.sleep(random.uniform(sleep_min, sleep_max))

        listing_url = next_url

    return list(company_infos.values())


def build_company_urls_from_infos(company_infos: Iterable[dict]) -> list[str]:
    """
    企業情報のリストから正規形の企業URLを生成する。
    """
    urls = [
        f"https://{BASE_DOMAIN}/cjlist{info['company_id']}/"
        for info in sorted(company_infos, key=lambda x: int(x['company_id']))
    ]
    return urls


if __name__ == "__main__":
    # 関東の一覧ページをスタートにする例
    start_url = "https://www.baitoru.com/kanto/jlist/"

    # ① 実際にクロールするのはこっち
    company_infos = crawl_baitoru_company_ids(
        start_listing_url=start_url,
        max_listing_pages=3,   # サイトの規模によって調整
        max_job_pages=50,      # 取りすぎ防止用の上限
        sleep_min=1.0,
        sleep_max=2.0,
    )

    # ② ここで使う変数名も company_infos に統一
    print(f"\n[RESULT] collected {len(company_infos)} companies\n")

    now = datetime.now().isoformat(timespec="seconds")
    out_path = "company_urls.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "company_id",
            "job_url",          # 求人詳細ページ
            "company_url",      # cjlist ページ
            "collected_at",
            "source_listing_url",
        ])
        for info in company_infos:
            writer.writerow([
                info["company_id"],
                info.get("source_job_url", ""),
                info.get("company_url", ""),
                now,
                info.get("source_listing_url", start_url),
            ])

    print(f"[RESULT] saved {len(company_infos)} company urls to {out_path}")

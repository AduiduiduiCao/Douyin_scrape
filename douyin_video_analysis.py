# -*- coding: utf-8 -*-
import json
import os
import re
import time
from typing import Dict, List, Optional, Set, Tuple, Any

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

# ====== 根据你本机情况改这两个路径 ======
CHROMEDRIVER_PATH = "/opt/homebrew/bin/chromedriver"
OUTPUT_JSON_PATH = "/Users/punic/douyin_video_stats/Douyin_analysis.json"

# ====== 启动 Chrome，手动登录 ======
def init_chrome_and_login():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)  # ✅ 正确写法

    driver.get("https://www.douyin.com/")
    print("[-] Chrome 已打开抖音首页，请在 Chrome 中手动完成 登录/扫码/短信验证。")
    input("[-] 完成登录后，不要关闭浏览器窗口，回到终端按 Enter 继续。\n>>> 登录完成后，在此终端按 Enter 继续... ")

    cookies = driver.get_cookies()
    cookie_dict = {c["name"]: c["value"] for c in cookies}
    print(f"[*] 已从 Chrome 获取到 {len(cookies)} 条 Cookie。")
    return driver, cookie_dict


# ====== 解析当前页面 HTML，正则抽取视频 ID ======
VIDEO_ID_RE = re.compile(r"/video/(\d{10,20})")
MODAL_ID_RE = re.compile(r"modal_id=(\d{10,20})")

def collect_ids_from_html(
    html: str,
    page_label: str,
    global_ids: Set[str],
    max_total: int
) -> int:
    """
    从整页 HTML 中用正则抽出视频 ID：
      - /video/{id}
      - modal_id={id}
    并更新 global_ids。
    """
    ids_video = VIDEO_ID_RE.findall(html)
    ids_modal = MODAL_ID_RE.findall(html)
    combined_ids = ids_video + ids_modal

    before = len(global_ids)
    for vid in combined_ids:
        if len(global_ids) >= max_total:
            break
        global_ids.add(vid)

    added = len(global_ids) - before
    print(
        f"    [调试] {page_label} 本轮 HTML 扫描到 /video {len(ids_video)} 个, "
        f"modal_id {len(ids_modal)} 个，新增作品 {added} 个，总计 {len(global_ids)}"
    )
    return added


def scroll_and_collect_on_page(
    driver: webdriver.Chrome,
    url: str,
    page_label: str,
    global_ids: Set[str],
    max_total: int,
) -> None:
    """
    在指定页面（精选 / 推荐）中不断下拉，直到：
      - 连续两轮无新增（中间带一次 3 秒暂停再滚动的重试），或
      - 总数达到 max_total
    """
    print(f"[*] 打开{page_label}页: {url}")
    driver.get(url)
    time.sleep(3)

    no_new_rounds = 0

    while len(global_ids) < max_total:
        html = driver.page_source
        added = collect_ids_from_html(html, page_label, global_ids, max_total)

        if added == 0:
            if no_new_rounds == 0:
                no_new_rounds = 1
                print(f"[*] {page_label} 本轮未发现新视频（首次），暂停 3 秒后再尝试一次下拉...")
                time.sleep(3)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                continue
            else:
                print(f"[*] {page_label} 暂停后再次下拉仍无新增，停止该页抓取。")
                break
        else:
            no_new_rounds = 0  # 有新增则重置

        if len(global_ids) >= max_total:
            print(f"[*] 已达到设定的最大视频数 {max_total}，停止该页抓取。")
            break

        print(f"[*] 下拉滚动加载更多（{page_label}）...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)


# ====== 请求详情页，解析 RENDER_DATA ======
def get_render_data_from_html(html: str) -> Optional[Any]:
    """
    从 HTML 中提取 script#RENDER_DATA 的 JSON。
    """
    m = re.search(
        r'<script id="RENDER_DATA" type="application/json">(.*?)</script>',
        html,
        re.S,
    )
    if not m:
        return None
    raw = m.group(1)
    # RENDER_DATA 里一般是 URL 编码过的 JSON
    try:
        from urllib.parse import unquote
        decoded = unquote(raw)
        return json.loads(decoded)
    except Exception:
        try:
            return json.loads(raw)
        except Exception:
            return None


def walk_find_aweme_nodes(obj: Any, found: List[Dict]) -> None:
    """
    递归遍历 JSON，收集疑似 aweme 结构的节点：
    同时带 awemeId/awemeIdStr/group_id/aweme_id 且带 stats/statistics。
    """
    if isinstance(obj, dict):
        keys = obj.keys()
        has_id_key = any(
            k in obj for k in ("awemeId", "awemeIdStr", "aweme_id", "group_id")
        )
        has_stats_key = any(k in obj for k in ("stats", "statistics"))
        if has_id_key and has_stats_key:
            found.append(obj)

        for v in obj.values():
            walk_find_aweme_nodes(v, found)

    elif isinstance(obj, list):
        for item in obj:
            walk_find_aweme_nodes(item, found)


def parse_aweme_from_render_data(
    data: Any, target_id: Optional[str] = None
) -> Optional[Dict]:
    """
    从 RENDER_DATA JSON 中找出目标 aweme 的概要信息：
      - aweme_id, title/desc, author.nickname, digg/comment/share/collect/play
    优先根据 target_id 匹配；否则取第一个。
    """
    candidates: List[Dict] = []
    walk_find_aweme_nodes(data, candidates)
    if not candidates:
        return None

    chosen = None

    def get_id(d: Dict) -> Optional[str]:
        return str(
            d.get("awemeId")
            or d.get("awemeIdStr")
            or d.get("aweme_id")
            or d.get("group_id")
            or ""
        ) or None

    if target_id is not None:
        for c in candidates:
            cid = get_id(c)
            if cid == target_id:
                chosen = c
                break

    if chosen is None:
        chosen = candidates[0]

    vid = get_id(chosen) or (target_id or "")

    # 标题
    title = (
        chosen.get("desc")
        or chosen.get("title")
        or chosen.get("awemeDesc")
        or ""
    )

    # 作者昵称
    author_name = ""
    if isinstance(chosen.get("author"), dict):
        author_name = (
            chosen["author"].get("nickname")
            or chosen["author"].get("nicknameName")
            or ""
        )

    # 统计字段
    stats = chosen.get("stats") or chosen.get("statistics") or {}
    digg = int(stats.get("diggCount") or stats.get("digg_count") or 0)
    comment = int(stats.get("commentCount") or stats.get("comment_count") or 0)
    share = int(stats.get("shareCount") or stats.get("share_count") or 0)
    collect = int(stats.get("collectCount") or stats.get("collect_count") or 0)
    play = int(stats.get("playCount") or stats.get("play_count") or 0)

    return {
        "aweme_id": vid,
        "title": title,
        "author": author_name,
        "digg_count": digg,
        "comment_count": comment,
        "share_count": share,
        "collect_count": collect,
        "play_count": play,
    }


def fetch_aweme_detail(
    session: requests.Session,
    cookies: Dict[str, str],
    aweme_id: str,
) -> Optional[Dict]:
    """
    通过 jingxuan 弹窗详情页获取该 aweme 的统计信息。
    """
    detail_url = f"https://www.douyin.com/jingxuan?modal_id={aweme_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.douyin.com/",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    try:
        resp = session.get(detail_url, headers=headers, cookies=cookies, timeout=10)
    except Exception as e:
        print(f"[!] 请求 {detail_url} 失败: {e}")
        return None

    if resp.status_code != 200:
        print(f"[!] 请求 {detail_url} 状态码异常: {resp.status_code}")
        return None

    data = get_render_data_from_html(resp.text)
    if not data:
        print(f"[!] {detail_url} 未解析出 RENDER_DATA")
        return None

    info = parse_aweme_from_render_data(data, target_id=aweme_id)
    if not info:
        print(f"[!] {detail_url} RENDER_DATA 中未找到匹配 awemeId 的节点")
        return None

    info["url"] = f"https://www.douyin.com/video/{aweme_id}"
    info["detail_url"] = detail_url
    print(
        f"    -> 解析成功：作品ID {info['aweme_id']} 点赞 {info['digg_count']} 评论 {info['comment_count']}"
    )
    return info


# ====== 主流程 ======
def main():
    driver, cookie_dict = init_chrome_and_login()

    session = requests.Session()

    all_video_ids: Set[str] = set()
    max_total = 500  # 你想要的总上限

    try:
        # 1) 先抓精选
        scroll_and_collect_on_page(
            driver,
            "https://www.douyin.com/jingxuan",
            "精选",
            all_video_ids,
            max_total,
        )

        # 2) 如果还没到上限，再抓推荐
        if len(all_video_ids) < max_total:
            scroll_and_collect_on_page(
                driver,
                "https://www.douyin.com/?recommend=1",
                "推荐",
                all_video_ids,
                max_total,
            )

        print(
            f"[*] 精选/推荐页共收集到 {len(all_video_ids)} 条视频（通过 HTML 正则）。"
        )

        if not all_video_ids:
            print("[!] 没从精选/推荐页收集到任何视频，程序结束。")
            return

        # 3) 遍历每个 aweme_id，拉详情页 RENDER_DATA
        results: List[Dict] = []
        for idx, vid in enumerate(sorted(all_video_ids), start=1):
            print(f"\n=== [{idx}/{len(all_video_ids)}] 处理视频 {vid} ===")
            info = fetch_aweme_detail(session, cookie_dict, vid)
            if info:
                results.append(info)

        # 4) 写入 JSON
        os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
        with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"[*] 已覆盖写入 JSON 到: {OUTPUT_JSON_PATH}")

        # 5) 再读一次打印摘要
        print("\n[*] 现在从 Douyin_analysis.json 中再读取并打印数据：\n")
        with open(OUTPUT_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        for i, item in enumerate(data, start=1):
            print(f"=== 从 Douyin_analysis.json 读取的视频 #{i} ===")
            print(f"原始链接: {item.get('url')}")
            print(f"详情链接: {item.get('detail_url')}")
            print(f"作品ID: {item.get('aweme_id')}")
            print(f"标题: {item.get('title')}")
            print(f"作者: {item.get('author')}")
            print(f"点赞: {item.get('digg_count')}")
            print(f"评论: {item.get('comment_count')}")
            print(f"分享: {item.get('share_count')}")
            print(f"收藏: {item.get('collect_count')}")
            print(f"播放: {item.get('play_count')}")
            print()

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
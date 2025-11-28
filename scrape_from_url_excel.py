#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ========= 可配置区域 =========
XLSX_PATH = "/Users/punic/douyin_video_stats/target_douyinURL.xlsx"
URL_COLUMN = "URL"

# 从第几行（0 基）开始处理；Excel 视觉上是“行号 - 1”
# 例如：83 行开始 -> START_ROW = 82
START_ROW = 1

# 最多处理多少行；None 表示从 START_ROW 一直处理到文件末尾
MAX_ROWS: Optional[int] = None

# 每个视频页面打开后等待多少秒，用于加载接口和收集 Network 日志
WAIT_AFTER_OPEN = 5

# 每个 URL 最多重试次数（包含第一次），用于处理“null”的情况
MAX_RETRY_PER_URL = 2

# 当出现 null / 接口缺失等错误时，重试前等待秒数
RETRY_WAIT_SECONDS = 5
# ==============================


def build_driver_with_network_logging() -> webdriver.Chrome:
    """启动带 performance 日志的 Chrome WebDriver，并开启 Network 获取 body 的能力。"""
    chrome_options = Options()
    # 如需无头可打开下一行
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    # 开启 performance 日志
    chrome_options.set_capability(
        "goog:loggingPrefs", {"performance": "ALL"}
    )

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)

    # 通过 CDP 启用 Network
    driver.execute_cdp_cmd("Network.enable", {})
    return driver


def extract_clean_url(raw: Any) -> Optional[str]:
    """从原始单元格内容中提取真正的 http(s) 链接."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None

    # 提取第一个 http(s) 开头的 URL
    m = re.search(r"(https?://[^\s]+)", text)
    if not m:
        return None

    url = m.group(1)
    # 去除末尾的一些无意义符号
    url = url.strip(".,;，。；")
    return url or None


def collect_json_responses(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """
    从 performance 日志中收集所有 JSON 响应：
    - 过滤 Network.responseReceived
    - MIME 为 application/json 或包含 json
    - 通过 requestId 调 Network.getResponseBody 拿 body
    """
    entries = driver.get_log("performance")
    results: List[Dict[str, Any]] = []

    for entry in entries:
        try:
            msg = json.loads(entry["message"])
            message = msg.get("message", {})
            method = message.get("method")
            if method != "Network.responseReceived":
                continue

            params = message.get("params", {})
            response = params.get("response", {})
            mime = (response.get("mimeType") or "").lower()
            if "json" not in mime:
                continue

            url = response.get("url", "")
            request_id = params.get("requestId")
            if not request_id:
                continue

            # 通过 CDP 取 body
            body_data = driver.execute_cdp_cmd(
                "Network.getResponseBody", {"requestId": request_id}
            )
            body = body_data.get("body") or ""
            if not body:
                continue

            try:
                data = json.loads(body)
            except Exception:
                # 不是合法 JSON，跳过
                continue

            results.append(
                {
                    "url": url,
                    "mime": mime,
                    "request_id": request_id,
                    "data": data,
                }
            )
        except Exception:
            # 单条日志解析异常，忽略
            continue

    return results


def find_aweme_detail_from_logs(
    driver: webdriver.Chrome,
) -> Optional[Dict[str, Any]]:
    """
    在当前页面的 Network JSON 响应中，寻找包含 aweme 统计数据的接口返回：
    - 优先匹配 aweme/v1/web/aweme/detail
    - 其次匹配任意包含 digg/comment/share/collect/play 字段的 JSON
    返回原始 JSON dict（可能含 aweme_detail 或 aweme_list）。
    """
    responses = collect_json_responses(driver)

    # 优先 detail 接口
    for resp in responses:
        url = resp["url"]
        data = resp["data"]
        if "/aweme/v1/web/aweme/detail" in url:
            if isinstance(data, dict) and (
                "aweme_detail" in data or "aweme_list" in data
            ):
                return data

    # 其次 favorite / related 等，里面也有 aweme_list + statistics
    target_keys = [
        "digg_count",
        "comment_count",
        "share_count",
        "collect_count",
        "play_count",
    ]
    for resp in responses:
        data = resp["data"]
        if not isinstance(data, dict):
            continue
        s = json.dumps(data, ensure_ascii=False)
        if all(k in s for k in target_keys):
            return data

    return None


def parse_stats_from_aweme_detail(
    data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    从 aweme detail / aweme list JSON 中解析统计数据。
    解析失败返回 None，调用方需要自行判断。
    """

    if not isinstance(data, dict):
        return None

    aweme = data.get("aweme_detail")

    # 如果 aweme_detail 为空，尝试从 aweme_list 里取第一个
    if not aweme:
        aweme_list = data.get("aweme_list") or []
        if isinstance(aweme_list, list) and aweme_list:
            aweme = aweme_list[0]

    # 仍然拿不到有效结构，放弃
    if not isinstance(aweme, dict):
        return None

    stats = aweme.get("statistics") or {}
    author = aweme.get("author") or {}

    return {
        "aweme_id": aweme.get("aweme_id"),
        "author": author.get("nickname"),
        "digg": stats.get("digg_count"),
        "comment": stats.get("comment_count"),
        "share": stats.get("share_count"),
        "collect": stats.get("collect_count"),
        "play": stats.get("play_count"),
    }


def ensure_stat_columns(df: pd.DataFrame) -> pd.DataFrame:
    """确保统计相关列存在，并统一为 object 类型，方便写入覆盖。"""
    stat_cols = [
        "aweme_id",
        "作者昵称",
        "点赞",
        "评论",
        "分享",
        "收藏",
        "播放量",
        "ok",
        "错误原因",
    ]
    for col in stat_cols:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = df[col].astype("object")
    return df


def main():
    # 1. 读取 Excel
    print(f"[*] 读取 Excel: {XLSX_PATH}")
    df = pd.read_excel(XLSX_PATH)

    # ==== 新增功能：如果没有 URL 列，则在列尾自动创建 ====
    if URL_COLUMN not in df.columns:
        print(
            f"[!] Excel 中未找到列 '{URL_COLUMN}'，已自动在最后新增该列（内容为空）。"
        )
        df[URL_COLUMN] = None
    else:
        print(f"[*] 检测到已有 '{URL_COLUMN}' 列，直接使用。")
    # =====================================================

    df = ensure_stat_columns(df)

    total_rows = len(df)
    start = max(0, START_ROW)
    if MAX_ROWS is None:
        end = total_rows
    else:
        end = min(total_rows, start + MAX_ROWS)

    print(f"[*] 本次计划处理 {end - start} 条记录（索引 {start} ~ {end - 1}）")

    # 2. 启动浏览器
    driver = build_driver_with_network_logging()

    try:
        # 先让你登录一次
        print("[*] 打开抖音首页进行登录...")
        driver.get("https://www.douyin.com/")
        input(">>> 请在浏览器中完成登录（扫码 / 账号密码等），完成后回到终端按 Enter 继续...\n")

        for idx in range(start, end):
            raw_url = df.at[idx, URL_COLUMN]
            excel_row_no = idx + 2  # 仅用于打印提示：含表头时大致对应 Excel 视觉行号

            url = extract_clean_url(raw_url)
            if not url:
                print(f"[{excel_row_no}] 无效 URL，跳过: {raw_url}")
                df.at[idx, "ok"] = False
                df.at[idx, "错误原因"] = "invalid_url"
                continue

            print(f"[{excel_row_no}] 处理 URL: {url}")

            last_error: Optional[str] = None
            stats: Optional[Dict[str, Any]] = None

            # === 重试逻辑：最多 MAX_RETRY_PER_URL 次 ===
            for attempt in range(1, MAX_RETRY_PER_URL + 1):
                try:
                    print(f"    [尝试 {attempt}/{MAX_RETRY_PER_URL}] 打开页面...")
                    driver.get(url)
                except Exception as e:
                    last_error = f"open_fail: {e}"
                    print(f"    [!] 打开页面失败: {e}")
                else:
                    print(f"[+] 打开页面: {url}")
                    time.sleep(WAIT_AFTER_OPEN)

                    # 从 Network 日志中寻找 aweme detail / stats
                    data = find_aweme_detail_from_logs(driver)
                    if not data:
                        last_error = "no_aweme_detail"
                        print("    [!] 未在 Network 日志中找到 aweme detail / stats 接口")
                    else:
                        stats = parse_stats_from_aweme_detail(data)
                        if not stats:
                            last_error = "parse_fail"
                            print("    [!] aweme_detail 结构异常，解析失败")
                        else:
                            # 检查统计字段是否全部为 None（即“null 情况”）
                            all_null = all(
                                stats.get(k) is None
                                for k in ("digg", "comment", "share", "collect", "play")
                            )
                            if all_null:
                                last_error = "all_null_stats"
                                print("    [!] 统计字段全部为 None，疑似 null，准备重试此 URL")
                            else:
                                # 成功获取有效数据
                                last_error = None
                                break  # 退出重试循环

                # 如果本次失败但还有机会重试
                if last_error is not None and attempt < MAX_RETRY_PER_URL:
                    print(
                        f"    [!] 当前尝试失败（{last_error}），"
                        f"等待 {RETRY_WAIT_SECONDS} 秒后重新加载当前 URL 再试..."
                    )
                    time.sleep(RETRY_WAIT_SECONDS)

            # === 重试结束后检查结果 ===
            if last_error is not None:
                # 多次尝试仍失败
                print(f"    [!] 多次尝试仍失败，记录错误并跳过（{last_error}）")
                df.at[idx, "ok"] = False
                df.at[idx, "错误原因"] = last_error
                continue

            if not stats:
                # 理论上不应走到这里，但防御一下
                print("    [!] 未获取到 stats，记录错误并跳过")
                df.at[idx, "ok"] = False
                df.at[idx, "错误原因"] = "stats_none_after_retry"
                continue

            # 成功解析，写回（覆盖旧值）
            df.at[idx, "aweme_id"] = (
                str(stats["aweme_id"]) if stats["aweme_id"] is not None else None
            )
            df.at[idx, "作者昵称"] = stats["author"]
            df.at[idx, "点赞"] = stats["digg"]
            df.at[idx, "评论"] = stats["comment"]
            df.at[idx, "分享"] = stats["share"]
            df.at[idx, "收藏"] = stats["collect"]
            df.at[idx, "播放量"] = stats["play"]
            df.at[idx, "ok"] = True
            df.at[idx, "错误原因"] = None

            print(
                f"    [结果] 作者={stats['author']}, "
                f"点赞={stats['digg']}, 评论={stats['comment']}, "
                f"分享={stats['share']}, 收藏={stats['collect']}, 播放={stats['play']}"
            )

        # 3. 保存 Excel（注意不要在 Excel 里打开文件）
        df.to_excel(XLSX_PATH, index=False)
        print(f"[*] 已更新并保存到: {XLSX_PATH}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
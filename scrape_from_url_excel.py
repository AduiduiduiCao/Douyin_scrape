# -*- coding: utf-8 -*-
"""
从 Douyin PC 视频页抓取真实统计数据：
- 读取 target_douyinURL.xlsx
- 如果没有 URL 列，则从 “发布链接” 列里解析出 https://v.douyin.com/... 短链接生成 URL 列
- 用 Selenium 打开视频页
- 利用 Network 日志找到 /aweme/v1/web/aweme/detail 接口
- 解析 aweme_detail.statistics 中的:
    digg_count / comment_count / share_count / collect_count / play_count
- 写回原 Excel（不新建文件）
- 新抓到的数据非 None 时覆盖旧值；为 None 时保留旧值
"""

import json
import time
import base64
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd
from selenium import webdriver

BASE_DIR = Path(__file__).resolve().parent
XLSX_PATH = BASE_DIR / "target_douyinURL.xlsx"

# 控制最多处理多少条，测试时可以用 5 / 20，稳定后改为 None 处理全部
MAX_ROWS: Optional[int] = 5


# ========== DataFrame 工具 ==========

def ensure_object_columns(df: pd.DataFrame, cols) -> None:
    """确保这些列存在且 dtype 为 object，避免写入 int/bool 时 FutureWarning。"""
    for col in cols:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype("object")


def extract_url_from_text(text: Any) -> Optional[str]:
    """从“发布链接”原始字符串里提取短链接 https://v.douyin.com/..."""
    if not isinstance(text, str):
        return None
    m = re.search(r"(https?://v\.douyin\.com/[^\s]+)", text)
    if not m:
        return None
    url = m.group(1).strip()
    return url


def ensure_url_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    如果已有 'URL' 列则直接使用；
    如果没有，则尝试从 '发布链接' 列解析生成 'URL' 列。
    """
    if "URL" in df.columns:
        df["URL"] = df["URL"].astype("object")
        print("[*] 检测到已有 'URL' 列，直接使用。")
        return df

    if "发布链接" not in df.columns:
        raise ValueError("Excel 中既没有 'URL' 列，也没有 '发布链接' 列，无法生成 URL。")

    print("[*] 未检测到 'URL' 列，尝试从 '发布链接' 列解析短链接生成 'URL' 列...")
    urls = []
    for raw in df["发布链接"]:
        url = extract_url_from_text(raw)
        urls.append(url)

    df["URL"] = pd.Series(urls, index=df.index, dtype="object")
    print("[*] 已从 '发布链接' 列解析生成 'URL' 列。")
    return df


# ========== Selenium / Network 日志相关 ==========

def get_driver_with_network_logging() -> webdriver.Chrome:
    """启动带 Network / performance 日志的 Chrome WebDriver（Selenium 4 写法）。"""
    print("[*] 启动带 Network 日志的 Chrome WebDriver...")

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    # Selenium 4：通过 options 设置 loggingPrefs
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=options)
    # 开启 CDP Network
    driver.execute_cdp_cmd("Network.enable", {})
    return driver


def decode_body(body_data: Dict[str, Any]) -> str:
    """根据 base64Encoded 标志解码 ResponseBody。"""
    body = body_data.get("body", "")
    if not body:
        return ""
    if body_data.get("base64Encoded"):
        try:
            return base64.b64decode(body).decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return body


def parse_aweme_detail_from_logs(driver) -> Optional[Dict[str, Any]]:
    """
    从当前页面的 performance 日志中查找 /aweme/v1/web/aweme/detail 请求，
    解析出 aweme_id / 作者昵称 / 各项统计。
    """
    logs = driver.get_log("performance")

    for entry in logs:
        try:
            msg = json.loads(entry["message"])
        except Exception:
            continue

        message = msg.get("message", {})
        method = message.get("method")
        params = message.get("params", {})

        if method != "Network.responseReceived":
            continue

        resp = params.get("response", {})
        mime_type = resp.get("mimeType", "")
        resource_type = params.get("type", "")
        url = resp.get("url", "")

        # 只看 JSON 的 XHR/Fetch
        if mime_type != "application/json":
            continue
        if resource_type not in ("XHR", "Fetch"):
            continue

        # 必须是 aweme detail 接口，排除 related / favorite 等其他接口
        if "/aweme/v1/web/aweme/detail" not in url:
            continue

        request_id = params.get("requestId")
        if not request_id:
            continue

        # 用 CDP API 拿响应 body
        try:
            body_data = driver.execute_cdp_cmd(
                "Network.getResponseBody", {"requestId": request_id}
            )
        except Exception:
            continue

        text = decode_body(body_data)
        if not text:
            continue

        try:
            data = json.loads(text)
        except Exception:
            continue

        detail = data.get("aweme_detail") or {}
        if not detail:
            continue

        stats = detail.get("statistics") or {}
        author = (detail.get("author") or {}).get("nickname")
        aweme_id = detail.get("aweme_id")

        result = {
            "aweme_id": aweme_id,
            "author": author,
            "digg_count": stats.get("digg_count"),
            "comment_count": stats.get("comment_count"),
            "share_count": stats.get("share_count"),
            "collect_count": stats.get("collect_count"),
            "play_count": stats.get("play_count"),
        }

        return result

    # 没找到 detail 接口
    return None


def scroll_page(driver):
    """简单滚动几次，触发懒加载 / 请求。"""
    time.sleep(2)
    for y in (400, 1200, 2000):
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(1)


def process_one_url(driver, url: str, idx: int) -> Tuple[bool, Dict[str, Any]]:
    """处理单个视频 URL，返回 (成功与否, 统计结果字典或错误信息)。"""
    print(f"[{idx}] 处理 URL: {url}")

    # 先清空旧的 performance 日志，避免前一个视频的请求干扰
    try:
        _ = driver.get_log("performance")
    except Exception:
        pass

    try:
        driver.get(url)
        print(f"[+] 打开页面: {url}")
    except Exception as e:
        return False, {"error": f"打开页面失败: {e}"}

    scroll_page(driver)

    # 再等待一会儿，给接口足够时间
    time.sleep(4)

    stats = parse_aweme_detail_from_logs(driver)
    if stats is None:
        return False, {"error": "未在 Network 日志中找到 aweme detail 接口"}

    print(
        f"    [结果] aweme_id={stats['aweme_id']}, "
        f"作者={stats['author']}, "
        f"点赞={stats['digg_count']}, 评论={stats['comment_count']}, "
        f"分享={stats['share_count']}, 收藏={stats['collect_count']}, 播放={stats['play_count']}"
    )
    return True, stats


# ========== 主流程 ==========

def main():
    if not XLSX_PATH.exists():
        print(f"[X] 未找到 Excel 文件: {XLSX_PATH}")
        return

    df = pd.read_excel(XLSX_PATH)

    # 先确保有 URL 列；没有就从 “发布链接” 解析
    try:
        df = ensure_url_column(df)
    except ValueError as e:
        print(f"[X] {e}")
        return

    # 准备要写入的列（使用 object 类型避免 dtype 的 FutureWarning）
    ensure_object_columns(
        df,
        [
            "aweme_id",
            "作者昵称",
            "点赞",
            "评论",
            "分享",
            "收藏",
            "播放",
            "ok",
        ],
    )

    driver = get_driver_with_network_logging()

    try:
        # 1. 先登录一次
        print("[*] 打开抖音首页进行登录...")
        driver.get("https://www.douyin.com/")
        print(">>> 请在浏览器中完成登录（扫码 / 账号密码等），完成后回到终端按 Enter 继续...")
        input()

        # 登录阶段产生的 performance 日志清掉
        try:
            _ = driver.get_log("performance")
        except Exception:
            pass

        ok_cnt = 0
        fail_cnt = 0

        total = len(df)
        limit = MAX_ROWS if MAX_ROWS is not None else total
        print(f"[*] 本次计划处理 {min(limit, total)} 条记录")

        for i, (row_idx, row) in enumerate(df.iterrows(), start=1):
            if i > limit:
                break

            url = row.get("URL")
            if not isinstance(url, str) or not url.startswith("http"):
                print(f"[{i}] 无效 URL，跳过:", url)
                continue

            success, info = process_one_url(driver, url, i)
            if success:
                # 智能覆盖：新值不是 None 时才覆盖旧值
                mapping = [
                    ("aweme_id", "aweme_id"),
                    ("作者昵称", "author"),
                    ("点赞", "digg_count"),
                    ("评论", "comment_count"),
                    ("分享", "share_count"),
                    ("收藏", "collect_count"),
                    ("播放", "play_count"),
                ]
                for col, key in mapping:
                    new_val = info.get(key)
                    if new_val is not None:
                        df.at[row_idx, col] = new_val

                df.at[row_idx, "ok"] = True
                ok_cnt += 1
            else:
                print(f"    [!] 失败原因: {info.get('error')}")
                # 失败只标记 ok=False，不动原有统计数据
                df.at[row_idx, "ok"] = False
                fail_cnt += 1

            # 简单延时，降低一点频率
            time.sleep(1.5)

        # 写回同一个 Excel 文件（包含新生成的 URL 列 + 统计列）
        df.to_excel(XLSX_PATH, index=False)
        print(
            f"[*] 统计完成，成功 {ok_cnt} 条，失败 {fail_cnt} 条，"
            f"结果已写回: {XLSX_PATH}"
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
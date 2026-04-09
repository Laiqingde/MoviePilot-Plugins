"""
网盘资源搜索插件
支持 PanSou / yz_pansearch / Nullbr 多后端搜索
支持 115、123、夸克、百度等网盘类型
"""
import concurrent.futures
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.event import eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

# ============================================================
# 网盘类型映射
# ============================================================
CLOUD_TYPE_MAP = {
    "baidu": "baidu", "百度": "baidu", "百度网盘": "baidu", "bd": "baidu",
    "quark": "quark", "夸克": "quark", "夸克网盘": "quark",
    "115": "115", "115网盘": "115",
    "123": "123", "123网盘": "123", "123pan": "123",
    "aliyun": "aliyun", "阿里": "aliyun", "阿里云盘": "aliyun",
    "xunlei": "xunlei", "迅雷": "xunlei", "迅雷云盘": "xunlei",
    "uc": "uc", "UC": "uc", "UC网盘": "uc",
    "pikpak": "pikpak", "PikPak": "pikpak",
    "tianyi": "tianyi", "天翼": "tianyi", "天翼云盘": "tianyi",
}

CLOUD_TYPE_DISPLAY = {
    "115": "115网盘",
    "123": "123网盘",
    "quark": "夸克网盘",
    "baidu": "百度网盘",
    "aliyun": "阿里云盘",
    "xunlei": "迅雷云盘",
    "uc": "UC网盘",
    "pikpak": "PikPak",
    "tianyi": "天翼云盘",
}


def normalize_cloud_type(raw: str) -> str:
    """将各种网盘类型字符串标准化"""
    if not raw:
        return "unknown"
    return CLOUD_TYPE_MAP.get(raw.strip(), raw.strip().lower())


# ============================================================
# 后端抽象基类
# ============================================================
class CloudSearchBackend(ABC):
    """网盘搜索后端基类"""

    def __init__(self, config: dict):
        self.base_url = config.get("base_url", "").rstrip("/")
        self.timeout = config.get("timeout", 15)

    @abstractmethod
    def search(self, keyword: str, cloud_types: List[str],
               page: int = 1) -> List[dict]:
        """
        搜索网盘资源
        返回统一格式:
        [{title, description, cloud_type, url, password, date, source_backend}]
        """
        raise NotImplementedError

    @abstractmethod
    def test_connection(self) -> bool:
        """测试后端连接"""
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


# ============================================================
# PanSou 后端
# ============================================================
class PanSouBackend(CloudSearchBackend):

    def __init__(self, config: dict):
        super().__init__(config)
        self.username = config.get("username", "")
        self.password = config.get("password", "")
        self._jwt_token = ""

    @property
    def name(self) -> str:
        return "PanSou"

    def _login(self) -> str:
        """JWT 登录获取 token"""
        if not self.username or not self.password:
            return ""
        try:
            resp = requests.post(
                f"{self.base_url}/api/auth/login",
                json={
                    "username": self.username,
                    "password": self.password,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                self._jwt_token = resp.json().get("token", "")
                return self._jwt_token
        except Exception as e:
            logger.warning(f"PanSou 登录失败: {e}")
        return ""

    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if not self._jwt_token and self.username:
            self._login()
        if self._jwt_token:
            headers["Authorization"] = f"Bearer {self._jwt_token}"
        return headers

    def search(self, keyword: str, cloud_types: List[str],
               page: int = 1) -> List[dict]:
        results = []
        try:
            payload = {
                "kw": keyword,
                "res": "all",
            }

            resp = requests.post(
                f"{self.base_url}/api/search",
                json=payload,
                headers=self._headers(),
                timeout=self.timeout,
            )
            # token 过期时重新登录重试
            if resp.status_code == 401 and self.username:
                self._jwt_token = ""
                resp = requests.post(
                    f"{self.base_url}/api/search",
                    json=payload,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            resp.raise_for_status()
            data = resp.json()

            # PanSou 返回 data.merged_by_type 按网盘类型分组
            resp_data = data.get("data", data)
            merged = resp_data.get("merged_by_type", {})
            for ctype, items in merged.items():
                normalized = normalize_cloud_type(ctype)
                if cloud_types and normalized not in cloud_types:
                    continue
                for item in (items or []):
                    if isinstance(item, dict):
                        results.append({
                            "title": item.get("note", "")
                                     or item.get("title", ""),
                            "description": item.get("source", ""),
                            "cloud_type": normalized,
                            "url": item.get("url", "")
                                   or item.get("link", ""),
                            "password": item.get("password", ""),
                            "date": item.get("datetime", ""),
                            "source_backend": self.name,
                        })

            # 也处理顶层 results 数组（如有）
            for item in resp_data.get("results", []):
                link = item.get("link", "") or item.get("url", "")
                cloud_type = self._detect_cloud_type(
                    link, item.get("source", ""))
                if cloud_types and cloud_type not in cloud_types:
                    continue
                results.append({
                    "title": item.get("note", "")
                             or item.get("title", ""),
                    "description": item.get("source", ""),
                    "cloud_type": cloud_type,
                    "url": link,
                    "password": item.get("password", ""),
                    "date": item.get("datetime", ""),
                    "source_backend": self.name,
                })

        except requests.exceptions.Timeout:
            logger.warning(f"PanSou 搜索超时: {keyword}")
        except requests.exceptions.ConnectionError:
            logger.error(f"PanSou 连接失败: {self.base_url}")
        except Exception as e:
            logger.error(f"PanSou 搜索异常: {e}")

        return results

    def test_connection(self) -> bool:
        try:
            resp = requests.get(
                f"{self.base_url}/api/health",
                headers=self._headers(),
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _detect_cloud_type(url: str, source: str = "") -> str:
        """从 URL 或来源信息推断网盘类型"""
        url_lower = (url or "").lower()
        text = f"{url_lower} {source}".lower()
        if "115" in text:
            return "115"
        if "123" in text or "123pan" in text:
            return "123"
        if "quark" in text:
            return "quark"
        if "baidu" in text or "pan.baidu" in text or "百度" in text:
            return "baidu"
        if "aliyun" in text or "alipan" in text or "阿里" in text:
            return "aliyun"
        if "xunlei" in text or "迅雷" in text:
            return "xunlei"
        if "uc" in text:
            return "uc"
        if "pikpak" in text:
            return "pikpak"
        return "unknown"


# ============================================================
# yz_pansearch 后端
# ============================================================
class YzPanSearchBackend(CloudSearchBackend):

    # yz_pansearch 支持的搜索源
    SOURCES = ["kk", "pansearch", "dj", "xxq"]
    # PAN-TYPE 到标准类型的映射
    PAN_TYPE_MAP = {
        "quark": "quark",
        "baidu": "baidu",
        "xunlei": "xunlei",
    }

    def __init__(self, config: dict):
        super().__init__(config)
        self.token = config.get("token", "")

    @property
    def name(self) -> str:
        return "yz_pansearch"

    def _headers(self, pan_type: str = "") -> dict:
        headers = {
            "APP-ID": "yz_pansearch_api",
            "APP-TOKEN": self.token,
            "Content-Type": "application/json",
        }
        if pan_type:
            headers["PAN-TYPE"] = pan_type
        return headers

    def search(self, keyword: str, cloud_types: List[str],
               page: int = 1) -> List[dict]:
        results = []
        # 确定要查询的 pan_types
        target_types = []
        for ct in cloud_types:
            if ct in self.PAN_TYPE_MAP:
                target_types.append(ct)
            elif ct in self.PAN_TYPE_MAP.values():
                target_types.append(ct)

        # 如果没有匹配的类型，尝试不指定 PAN-TYPE 查询
        if not target_types:
            target_types = [""]

        for pan_type in target_types:
            for source in self.SOURCES:
                try:
                    resp = requests.post(
                        f"{self.base_url}/v1/search/get_{source}",
                        json={"kw": keyword},
                        headers=self._headers(pan_type),
                        timeout=self.timeout,
                    )
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    if data.get("status") != 0:
                        continue

                    rows = data.get("data", {}).get("rows", [])
                    for row in rows:
                        title = row.get("title", "")
                        desc = row.get("description", "")
                        res_dict = row.get("res_dict", {})

                        # 展开 res_dict 中各网盘类型
                        for rtype, links in res_dict.items():
                            normalized = normalize_cloud_type(rtype)
                            if cloud_types and normalized not in cloud_types:
                                continue
                            for link_info in (links or []):
                                if isinstance(link_info, dict):
                                    results.append({
                                        "title": title,
                                        "description": desc,
                                        "cloud_type": normalized,
                                        "url": link_info.get("url", ""),
                                        "password": link_info.get("code", ""),
                                        "date": "",
                                        "source_backend": self.name,
                                    })

                except requests.exceptions.Timeout:
                    logger.warning(
                        f"yz_pansearch 搜索超时: source={source}, "
                        f"pan_type={pan_type}")
                except requests.exceptions.ConnectionError:
                    logger.error(f"yz_pansearch 连接失败: {self.base_url}")
                    return results  # 连接失败直接返回
                except Exception as e:
                    logger.error(
                        f"yz_pansearch 搜索异常: source={source}, {e}")

        return results

    def test_connection(self) -> bool:
        try:
            resp = requests.post(
                f"{self.base_url}/v1/search/get_kk",
                json={"kw": "test"},
                headers=self._headers(),
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False


# ============================================================
# Nullbr 后端
# ============================================================
class NullbrBackend(CloudSearchBackend):

    def __init__(self, config: dict):
        super().__init__(config)
        self.app_id = config.get("app_id", "")
        self.api_key = config.get("api_key", "")

    @property
    def name(self) -> str:
        return "Nullbr"

    def _headers(self) -> dict:
        headers = {}
        if self.app_id:
            headers["APP-ID"] = self.app_id
        if self.api_key:
            headers["API-KEY"] = self.api_key
        return headers

    def search(self, keyword: str, cloud_types: List[str],
               page: int = 1) -> List[dict]:
        results = []
        try:
            resp = requests.get(
                f"{self.base_url}/nullbr/search",
                params={"keyword": keyword, "page": page},
                headers=self._headers(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            for item in items:
                title = item.get("title", "")
                year = item.get("year", "")
                tmdb_id = item.get("tmdb_id", "")
                media_type = item.get("media_type", "movie")

                # 检查各网盘类型标记
                has_115 = item.get("has_115", False)
                has_quark = item.get("has_quark", False)
                has_baidu = item.get("has_baidu", False)

                # 构建详情页 URL
                detail_url = (f"{self.base_url}/nullbr/"
                              f"{media_type}/{tmdb_id}/resources")

                cloud_hits = []
                if has_115 and ("115" in cloud_types or not cloud_types):
                    cloud_hits.append("115")
                if has_quark and ("quark" in cloud_types or not cloud_types):
                    cloud_hits.append("quark")
                if has_baidu and ("baidu" in cloud_types or not cloud_types):
                    cloud_hits.append("baidu")

                # 尝试获取前10个结果的具体链接
                if cloud_hits and len(results) < 10:
                    for ct in cloud_hits:
                        links = self._fetch_resources(
                            tmdb_id, media_type, ct)
                        for link in links:
                            results.append({
                                "title": f"{title} ({year})" if year
                                         else title,
                                "description": link.get("description", ""),
                                "cloud_type": ct,
                                "url": link.get("url", detail_url),
                                "password": link.get("password", ""),
                                "date": link.get("date", ""),
                                "source_backend": self.name,
                            })
                elif cloud_hits:
                    # 超过10条只返回详情页链接
                    for ct in cloud_hits:
                        results.append({
                            "title": f"{title} ({year})" if year else title,
                            "description": f"包含{CLOUD_TYPE_DISPLAY.get(ct, ct)}资源",
                            "cloud_type": ct,
                            "url": detail_url,
                            "password": "",
                            "date": "",
                            "source_backend": self.name,
                        })

        except requests.exceptions.Timeout:
            logger.warning(f"Nullbr 搜索超时: {keyword}")
        except requests.exceptions.ConnectionError:
            logger.error(f"Nullbr 连接失败: {self.base_url}")
        except Exception as e:
            logger.error(f"Nullbr 搜索异常: {e}")

        return results

    def _fetch_resources(self, tmdb_id: str, media_type: str,
                         cloud_type: str) -> List[dict]:
        """获取单个媒体的具体网盘资源链接"""
        try:
            resp = requests.get(
                f"{self.base_url}/nullbr/{media_type}/{tmdb_id}/resources",
                params={"type": cloud_type},
                headers=self._headers(),
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data.get("resources", [])
        except Exception as e:
            logger.debug(f"Nullbr 获取资源链接失败: {e}")
            return []

    def test_connection(self) -> bool:
        try:
            resp = requests.get(
                f"{self.base_url}/nullbr/test",
                headers=self._headers(),
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False


# ============================================================
# 主插件类
# ============================================================
class CloudDriveSearch(_PluginBase):
    # 插件元数据
    plugin_name = "网盘资源搜索"
    plugin_desc = "搜索网盘资源，支持PanSou/yz_pansearch/Nullbr多后端，" \
                  "支持115、123、夸克、百度等网盘"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/" \
                  "MoviePilot-Plugins/main/icons/clouddisk.png"
    plugin_version = "1.6.1"
    plugin_author = "早点下班"
    author_url = "https://github.com/Laiqingde"
    plugin_config_prefix = "clouddrivesearch_"
    plugin_order = 50
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _backends: list = []
    _pansou_url: str = ""
    _pansou_username: str = ""
    _pansou_password: str = ""
    _yz_url: str = ""
    _yz_token: str = ""
    _nullbr_base_url: str = "https://api.nullbr.eu.org"
    _nullbr_app_id: str = ""
    _nullbr_api_key: str = ""
    _cloud_types: list = []
    _search_in_system: bool = True
    _timeout: int = 15

    # 保存原始方法引用
    _original_async_search_by_title = None
    _original_async_search_by_id = None
    _patched: bool = False
    # 调用追踪
    _last_call_time: str = ""
    _last_call_keyword: str = ""
    _last_call_result_count: int = 0
    _call_count: int = 0

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._backends = config.get("backends") or []
            self._pansou_url = config.get("pansou_url", "")
            self._pansou_username = config.get("pansou_username", "")
            self._pansou_password = config.get("pansou_password", "")
            self._yz_url = config.get("yz_url", "")
            self._yz_token = config.get("yz_token", "")
            self._nullbr_base_url = config.get(
                "nullbr_base_url", "https://api.nullbr.eu.org")
            self._nullbr_app_id = config.get("nullbr_app_id", "")
            self._nullbr_api_key = config.get("nullbr_api_key", "")
            self._cloud_types = config.get("cloud_types") or [
                "115", "123", "quark", "baidu"]
            self._search_in_system = config.get("search_in_system", True)
            self._timeout = int(config.get("timeout") or 15)

        # Patch SearchChain 注入云盘搜索
        if self._enabled and self._search_in_system:
            self._patch_search_chain()
        else:
            self._unpatch_search_chain()

    def _patch_search_chain(self):
        """Monkey-patch SearchChain 的 async 搜索方法"""
        if self._patched:
            return
        try:
            from app.chain.search import SearchChain

            plugin = self

            # === Patch async_search_by_title ===
            if not CloudDriveSearch._original_async_search_by_title:
                CloudDriveSearch._original_async_search_by_title = \
                    SearchChain.async_search_by_title

            async def patched_async_search_by_title(
                    chain_self, title: str = None, **kwargs):
                original = \
                    CloudDriveSearch._original_async_search_by_title
                original_results = await original(
                    chain_self, title=title, **kwargs)
                if original_results is None:
                    original_results = []

                # 保存原始结果数，用于回退
                original_count = len(original_results)

                if not title:
                    return original_results

                try:
                    plugin._call_count += 1
                    plugin._last_call_time = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    plugin._last_call_keyword = title
                    logger.info(
                        f"[CloudDriveSearch] async_search_by_title: "
                        f"{title}")
                    from app.schemas.context import Context
                    from app.core.metainfo import MetaInfo

                    raw = plugin._do_search(keyword=title, page=1)
                    cloud_items = []
                    for item in raw:
                        try:
                            ti = plugin._to_torrent_info(item)
                            if ti:
                                # 包装为 Context（和原始结果格式一致）
                                meta = MetaInfo(
                                    title=ti.title,
                                    subtitle=ti.description)
                                ctx = Context(
                                    meta_info=meta,
                                    torrent_info=ti)
                                cloud_items.append(ctx)
                        except Exception as ie:
                            logger.debug(
                                f"[CloudDriveSearch] 跳过: {ie}")
                            continue

                    plugin._last_call_result_count = len(cloud_items)
                    original_results.extend(cloud_items)
                    logger.info(
                        f"[CloudDriveSearch] 云盘搜索完成: "
                        f"{len(cloud_items)} 条")
                except Exception as e:
                    logger.error(
                        f"[CloudDriveSearch] 异常: {e}",
                        exc_info=True)
                    # 回退：移除所有云盘结果
                    if len(original_results) > original_count:
                        del original_results[original_count:]

                return original_results

            SearchChain.async_search_by_title = \
                patched_async_search_by_title

            # === Patch async_search_by_id ===
            if not CloudDriveSearch._original_async_search_by_id:
                CloudDriveSearch._original_async_search_by_id = \
                    SearchChain.async_search_by_id

            async def patched_async_search_by_id(
                    chain_self, tmdbid: int = None,
                    doubanid: str = None, **kwargs):
                original = \
                    CloudDriveSearch._original_async_search_by_id
                original_results = await original(
                    chain_self, tmdbid=tmdbid,
                    doubanid=doubanid, **kwargs)
                if original_results is None:
                    original_results = []

                # 提取搜索关键词
                search_keyword = None
                try:
                    if original_results:
                        first = original_results[0]
                        if hasattr(first, 'media_info') and first.media_info:
                            mi = first.media_info
                            search_keyword = getattr(mi, 'title', None) \
                                or getattr(mi, 'name', None)
                except Exception:
                    pass

                if not search_keyword:
                    return original_results

                try:
                    from app.schemas.context import Context
                    from app.core.metainfo import MetaInfo

                    plugin._call_count += 1
                    plugin._last_call_time = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    plugin._last_call_keyword = search_keyword
                    logger.info(
                        f"[CloudDriveSearch] async_search_by_id: "
                        f"{search_keyword}")
                    raw = plugin._do_search(
                        keyword=search_keyword, page=1)
                    count = 0
                    media_info = original_results[0].media_info \
                        if original_results else None

                    for item in raw:
                        ti = plugin._to_torrent_info(item)
                        if ti:
                            try:
                                meta = MetaInfo(title=ti.title)
                                ctx = Context(
                                    torrent_info=ti,
                                    media_info=media_info,
                                    meta_info=meta)
                                original_results.append(ctx)
                                count += 1
                            except Exception as ce:
                                logger.debug(
                                    f"[CloudDriveSearch] "
                                    f"Context包装跳过: {ce}")

                    plugin._last_call_result_count = count
                    logger.info(
                        f"[CloudDriveSearch] 云盘搜索完成: {count} 条")
                except Exception as e:
                    logger.error(
                        f"[CloudDriveSearch] search_by_id异常: {e}")

                return original_results

            SearchChain.async_search_by_id = \
                patched_async_search_by_id

            self._patched = True
            logger.info(
                "[CloudDriveSearch] 已 patch SearchChain "
                "(async_search_by_id + async_search_by_title)")

        except Exception as e:
            logger.error(
                f"[CloudDriveSearch] patch 失败: {e}")

    def _unpatch_search_chain(self):
        """还原 SearchChain"""
        if not self._patched:
            return
        try:
            from app.chain.search import SearchChain
            if CloudDriveSearch._original_async_search_by_title:
                SearchChain.async_search_by_title = \
                    CloudDriveSearch._original_async_search_by_title
            if CloudDriveSearch._original_async_search_by_id:
                SearchChain.async_search_by_id = \
                    CloudDriveSearch._original_async_search_by_id
            self._patched = False
            logger.info("[CloudDriveSearch] 已还原 SearchChain")
        except Exception as e:
            logger.error(f"[CloudDriveSearch] 还原失败: {e}")

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        self._unpatch_search_chain()

    # --------------------------------------------------------
    # 系统搜索集成（保留 get_module 用于未来兼容）
    # --------------------------------------------------------
    def get_module(self) -> Dict[str, Any]:
        if self._enabled and self._search_in_system:
            return {"search_torrents": self._search_torrents_for_module}
        return {}

    def _search_torrents_for_module(self, site=None, keyword: str = None,
                                    mtype=None, page: int = 0,
                                    **kwargs) -> Optional[List[Any]]:
        """备用：如果框架支持 get_module 会走这个路径"""
        if not self._enabled or not keyword:
            return None
        try:
            from app.schemas.context import TorrentInfo
        except ImportError:
            return None

        self._call_count += 1
        self._last_call_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._last_call_keyword = keyword or ""

        raw = self._do_search(keyword=keyword, page=page + 1)
        all_results = []
        for item in raw:
            ti = self._to_torrent_info(item)
            if ti:
                all_results.append(ti)
        self._last_call_result_count = len(all_results)
        return all_results if all_results else None

    def _to_torrent_info(self, item: dict):
        """将统一结果转换为 TorrentInfo"""
        try:
            from app.schemas.context import TorrentInfo
        except ImportError:
            return None

        cloud_type = item.get("cloud_type", "unknown")
        cloud_display = CLOUD_TYPE_DISPLAY.get(cloud_type, cloud_type)
        backend = item.get("source_backend", "CloudDrive")
        password = item.get("password", "")
        pwd_info = f" | 提取码: {password}" if password else ""

        try:
            return TorrentInfo(
                title=item.get("title", "") or "未知资源",
                description=f"[{cloud_display}] "
                            f"{item.get('description', '')}{pwd_info}",
                enclosure=item.get("url", "") or "",
                page_url=item.get("url", "") or "",
                size=0,
                seeders=0,
                peers=0,
                site_name=f"{backend}-{cloud_display}",
                site=0,
                uploadvolumefactor=0.0,
                downloadvolumefactor=0.0,
            )
        except Exception as e:
            logger.debug(f"[CloudDriveSearch] TorrentInfo创建失败: {e}")
            return None

    # --------------------------------------------------------
    # 搜索核心
    # --------------------------------------------------------
    def _do_search(self, keyword: str, page: int = 1) -> List[dict]:
        """执行多后端并发搜索"""
        backends = self._get_active_backends()
        if not backends:
            logger.warning("没有配置可用的搜索后端")
            return []

        all_results = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max(len(backends), 1)) as executor:
            future_map = {
                executor.submit(
                    b.search, keyword, self._cloud_types, page
                ): b
                for b in backends
            }
            for future in concurrent.futures.as_completed(
                    future_map, timeout=self._timeout + 10):
                backend = future_map[future]
                try:
                    results = future.result(timeout=self._timeout)
                    all_results.extend(results)
                    logger.info(
                        f"{backend.name} 返回 {len(results)} 条结果")
                except Exception as e:
                    logger.warning(f"{backend.name} 搜索失败: {e}")

        # 按 URL 去重
        seen_urls = set()
        deduped = []
        for r in all_results:
            url = r.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped.append(r)
            elif not url:
                deduped.append(r)

        # 按网盘类型过滤
        if self._cloud_types:
            deduped = [
                r for r in deduped
                if r.get("cloud_type") in self._cloud_types
                or r.get("cloud_type") == "unknown"
            ]

        return deduped

    def _get_active_backends(self) -> List[CloudSearchBackend]:
        """获取已配置且可用的后端列表"""
        backends = []
        if "pansou" in self._backends and self._pansou_url:
            backends.append(PanSouBackend({
                "base_url": self._pansou_url,
                "username": self._pansou_username,
                "password": self._pansou_password,
                "timeout": self._timeout,
            }))
        if "yz_pansearch" in self._backends and self._yz_url:
            backends.append(YzPanSearchBackend({
                "base_url": self._yz_url,
                "token": self._yz_token,
                "timeout": self._timeout,
            }))
        if "nullbr" in self._backends:
            backends.append(NullbrBackend({
                "base_url": self._nullbr_base_url,
                "app_id": self._nullbr_app_id,
                "api_key": self._nullbr_api_key,
                "timeout": self._timeout,
            }))
        return backends

    # --------------------------------------------------------
    # API 端点
    # --------------------------------------------------------
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/search",
                "endpoint": self.api_search,
                "methods": ["GET"],
                "summary": "搜索网盘资源",
                "description": "根据关键词搜索网盘资源",
            },
            {
                "path": "/test_backends",
                "endpoint": self.api_test_backends,
                "methods": ["GET"],
                "summary": "测试后端连接",
                "description": "测试所有已配置后端的连接状态",
            },
            {
                "path": "/debug",
                "endpoint": self.api_debug,
                "methods": ["GET"],
                "summary": "诊断信息",
                "description": "返回插件状态和get_module信息",
            },
            {
                "path": "/test_torrent",
                "endpoint": self.api_test_torrent,
                "methods": ["GET"],
                "summary": "测试TorrentInfo序列化",
                "description": "测试TorrentInfo对象的序列化方法",
            },
        ]

    def api_search(self, keyword: str = "", page: int = 1) -> dict:
        """搜索 API 端点"""
        if not keyword:
            return {"code": 1, "msg": "请输入搜索关键词", "data": []}

        results = self._do_search(keyword=keyword, page=page)

        # 存储搜索结果供页面展示
        self.save_data("last_results", results[:200])
        self.save_data("last_keyword", keyword)

        return {
            "code": 0,
            "msg": "ok",
            "data": results,
            "total": len(results),
        }

    def api_test_backends(self) -> dict:
        """测试后端连接"""
        backends = self._get_active_backends()
        status = {}
        for b in backends:
            try:
                status[b.name] = b.test_connection()
            except Exception:
                status[b.name] = False
        return {"code": 0, "data": status}

    def api_test_torrent(self) -> dict:
        """测试 Context(TorrentInfo) 序列化"""
        try:
            from app.schemas.context import TorrentInfo, Context
            from app.core.metainfo import MetaInfo
            ti = TorrentInfo(
                title="测试资源",
                description="[115网盘] 测试",
                enclosure="https://example.com",
                page_url="https://example.com",
                size=0, seeders=0, peers=0,
                site_name="PanSou-115网盘",
                site=0,
                uploadvolumefactor=0.0,
                downloadvolumefactor=0.0,
            )
            meta = MetaInfo(title=ti.title, subtitle=ti.description)
            ctx = Context(meta_info=meta, torrent_info=ti)
            # 测试 to_dict
            ctx_dict = ctx.to_dict()
            return {
                "code": 0,
                "context_to_dict_ok": True,
                "context_keys": list(ctx_dict.keys()) if isinstance(ctx_dict, dict) else str(type(ctx_dict)),
                "torrent_title": ctx_dict.get("torrent_info", {}).get("title", "?") if isinstance(ctx_dict, dict) else "?",
            }
        except Exception as e:
            import traceback
            return {"code": 1, "error": str(e), "trace": traceback.format_exc()}

    def api_debug(self) -> dict:
        """诊断信息"""
        module_result = self.get_module()
        has_search = "search_torrents" in module_result if module_result else False
        return {
            "enabled": self._enabled,
            "search_in_system": self._search_in_system,
            "backends": self._backends,
            "cloud_types": self._cloud_types,
            "timeout": self._timeout,
            "get_module_keys": list(module_result.keys()) if module_result else [],
            "has_search_torrents": has_search,
            "get_state": self.get_state(),
            "pansou_url": self._pansou_url,
            "yz_url": self._yz_url,
            "active_backends": [b.name for b in self._get_active_backends()],
            "search_chain_patched": self._patched,
            "call_tracking": {
                "call_count": self._call_count,
                "last_call_time": self._last_call_time,
                "last_call_keyword": self._last_call_keyword,
                "last_call_result_count": self._last_call_result_count,
            },
        }

    # --------------------------------------------------------
    # 远程命令
    # --------------------------------------------------------
    def get_command(self) -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/cloud_search",
                "event": EventType.PluginAction,
                "desc": "网盘资源搜索",
                "category": "搜索",
                "data": {"action": "cloud_search"},
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event):
        """处理远程命令"""
        if not event:
            return
        event_data = event.event_data or {}
        if event_data.get("action") != "cloud_search":
            return

        keyword = event_data.get("text", "").strip()
        if not keyword:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="网盘资源搜索",
                text="请输入搜索关键词，格式: /cloud_search 关键词",
            )
            return

        results = self._do_search(keyword=keyword)
        if not results:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="网盘资源搜索",
                text=f"未找到 [{keyword}] 的网盘资源",
            )
            return

        # 组装消息
        msg_lines = [f"搜索 [{keyword}] 找到 {len(results)} 条结果:\n"]
        for i, r in enumerate(results[:10]):
            cloud_display = CLOUD_TYPE_DISPLAY.get(
                r["cloud_type"], r["cloud_type"])
            pwd = f" 提取码:{r['password']}" if r.get("password") else ""
            msg_lines.append(
                f"{i + 1}. [{cloud_display}] {r['title']}\n"
                f"   {r['url']}{pwd}"
            )

        if len(results) > 10:
            msg_lines.append(f"\n... 共 {len(results)} 条结果")

        self.post_message(
            mtype=NotificationType.Plugin,
            title="网盘资源搜索",
            text="\n".join(msg_lines),
        )

    # --------------------------------------------------------
    # 定时服务（无）
    # --------------------------------------------------------
    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # --------------------------------------------------------
    # 独立搜索页面
    # --------------------------------------------------------
    def get_page(self) -> List[dict]:
        last_results = self.get_data("last_results") or []
        last_keyword = self.get_data("last_keyword") or ""

        # 构建结果表格行
        table_rows = []
        for r in last_results:
            cloud_display = CLOUD_TYPE_DISPLAY.get(
                r.get("cloud_type", ""), r.get("cloud_type", ""))
            pwd = r.get("password", "")
            table_rows.append({
                "component": "tr",
                "content": [
                    {
                        "component": "td",
                        "props": {"class": "text-subtitle-2"},
                        "text": r.get("title", ""),
                    },
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "color": self._cloud_color(
                                        r.get("cloud_type", "")),
                                },
                                "text": cloud_display,
                            }
                        ],
                    },
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "a",
                                "props": {
                                    "href": r.get("url", ""),
                                    "target": "_blank",
                                    "class": "text-primary",
                                },
                                "text": "打开链接",
                            }
                        ],
                    },
                    {
                        "component": "td",
                        "text": pwd if pwd else "-",
                    },
                    {
                        "component": "td",
                        "text": r.get("date", "-") or "-",
                    },
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "x-small",
                                    "variant": "outlined",
                                },
                                "text": r.get("source_backend", ""),
                            }
                        ],
                    },
                ],
            })

        if not table_rows:
            table_rows = [
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "props": {"colspan": 6,
                                      "class": "text-center text-grey"},
                            "text": "暂无搜索结果，请通过 API 搜索: "
                                    "/api/v1/plugin/CloudDriveSearch/"
                                    "search?keyword=关键词",
                        }
                    ],
                }
            ]

        keyword_text = f"上次搜索: {last_keyword}" if last_keyword else "网盘资源搜索"

        return [
            {
                "component": "VCard",
                "props": {"class": "mb-4"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "text": keyword_text,
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "class": "mb-4",
                                },
                                "text": "网盘资源链接需要手动打开保存，"
                                        "无法通过下载器自动下载。"
                                        "开启\"集成到系统搜索\"后，"
                                        "搜索结果也会出现在系统搜索列表中。",
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VTable",
                        "props": {"hover": True, "density": "comfortable"},
                        "content": [
                            {
                                "component": "thead",
                                "content": [
                                    {
                                        "component": "tr",
                                        "content": [
                                            {"component": "th",
                                             "text": "标题"},
                                            {"component": "th",
                                             "text": "网盘类型"},
                                            {"component": "th",
                                             "text": "链接"},
                                            {"component": "th",
                                             "text": "提取码"},
                                            {"component": "th",
                                             "text": "日期"},
                                            {"component": "th",
                                             "text": "来源"},
                                        ],
                                    }
                                ],
                            },
                            {
                                "component": "tbody",
                                "content": table_rows,
                            },
                        ],
                    }
                ],
            },
        ]

    @staticmethod
    def _cloud_color(cloud_type: str) -> str:
        """网盘类型对应的颜色"""
        colors = {
            "115": "purple",
            "123": "blue",
            "quark": "orange",
            "baidu": "blue-darken-3",
            "aliyun": "orange-darken-2",
            "xunlei": "blue-lighten-1",
        }
        return colors.get(cloud_type, "grey")

    # --------------------------------------------------------
    # 配置表单
    # --------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return (
            [
                {
                    "component": "VForm",
                    "content": [
                        # 基本设置
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 3},
                                    "content": [
                                        {
                                            "component": "VSwitch",
                                            "props": {
                                                "model": "enabled",
                                                "label": "启用插件",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 3},
                                    "content": [
                                        {
                                            "component": "VSwitch",
                                            "props": {
                                                "model": "search_in_system",
                                                "label": "集成到系统搜索",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 3},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "timeout",
                                                "label": "超时时间(秒)",
                                                "type": "number",
                                                "placeholder": "15",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        # 后端选择 + 网盘类型
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 6},
                                    "content": [
                                        {
                                            "component": "VSelect",
                                            "props": {
                                                "model": "backends",
                                                "label": "搜索后端",
                                                "multiple": True,
                                                "chips": True,
                                                "closable-chips": True,
                                                "items": [
                                                    {
                                                        "title": "PanSou（推荐）",
                                                        "value": "pansou",
                                                    },
                                                    {
                                                        "title": "yz_pansearch",
                                                        "value": "yz_pansearch",
                                                    },
                                                    {
                                                        "title": "Nullbr",
                                                        "value": "nullbr",
                                                    },
                                                ],
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 6},
                                    "content": [
                                        {
                                            "component": "VSelect",
                                            "props": {
                                                "model": "cloud_types",
                                                "label": "网盘类型",
                                                "multiple": True,
                                                "chips": True,
                                                "closable-chips": True,
                                                "items": [
                                                    {
                                                        "title": "115网盘",
                                                        "value": "115",
                                                    },
                                                    {
                                                        "title": "123网盘",
                                                        "value": "123",
                                                    },
                                                    {
                                                        "title": "夸克网盘",
                                                        "value": "quark",
                                                    },
                                                    {
                                                        "title": "百度网盘",
                                                        "value": "baidu",
                                                    },
                                                ],
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        # === PanSou 配置 ===
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VDivider",
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VAlert",
                                            "props": {
                                                "type": "info",
                                                "variant": "tonal",
                                                "text": "PanSou 配置 - "
                                                        "需自建Docker服务 "
                                                        "(github.com/fish2018"
                                                        "/pansou)",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "pansou_url",
                                                "label": "PanSou API地址",
                                                "placeholder":
                                                    "http://192.168.1.100"
                                                    ":8888",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "pansou_username",
                                                "label": "用户名 (可选)",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "pansou_password",
                                                "label": "密码 (可选)",
                                                "type": "password",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        # === yz_pansearch 配置 ===
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VDivider",
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VAlert",
                                            "props": {
                                                "type": "info",
                                                "variant": "tonal",
                                                "text": "yz_pansearch 配置 - "
                                                        "需自建Docker服务 "
                                                        "(github.com/"
                                                        "fre123-com/"
                                                        "yz_pansearch_api)",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 8},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "yz_url",
                                                "label":
                                                    "yz_pansearch API地址",
                                                "placeholder":
                                                    "http://192.168.1.100"
                                                    ":8067",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "yz_token",
                                                "label": "APP-TOKEN",
                                                "type": "password",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        # === Nullbr 配置 ===
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VDivider",
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VAlert",
                                            "props": {
                                                "type": "info",
                                                "variant": "tonal",
                                                "text": "Nullbr 配置 - "
                                                        "云端API，"
                                                        "无需自建服务",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "nullbr_base_url",
                                                "label": "Nullbr API地址",
                                                "placeholder":
                                                    "https://api.nullbr"
                                                    ".eu.org",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "nullbr_app_id",
                                                "label": "APP-ID",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        {
                                            "component": "VTextField",
                                            "props": {
                                                "model": "nullbr_api_key",
                                                "label": "API-KEY",
                                                "type": "password",
                                            },
                                        }
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
            # 默认配置值
            {
                "enabled": False,
                "search_in_system": True,
                "timeout": 15,
                "backends": [],
                "cloud_types": ["115", "123", "quark", "baidu"],
                "pansou_url": "",
                "pansou_username": "",
                "pansou_password": "",
                "yz_url": "",
                "yz_token": "",
                "nullbr_base_url": "https://api.nullbr.eu.org",
                "nullbr_app_id": "",
                "nullbr_api_key": "",
            },
        )

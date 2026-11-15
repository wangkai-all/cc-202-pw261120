"""确定性中文地址拆分器。

设计原则：
1. 纯函数、无随机、无外部依赖：同一输入永远得到同一输出，在线双写与历史回填
   共用这一份规则，因此两套投影不可能因"触发顺序"产生差异。
2. 只输出原文中真实存在的片段（逐字提取），识别不了就留空并标记状态，
   绝不编造任何字段。
3. 三态结果：
   - parsed  : 行政层级 + 街道/门牌号等细节足够可靠
   - partial : 拆出了部分字段，但关键层级缺失
   - pending : 无法可靠拆分，保留原文等待人工处理
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

# ---- 行政区划词表（长名优先匹配，匹配到的都是原文逐字片段） ----------------

PROVINCES = [
    # 直辖市 / 特别行政区 / 自治区（全称在前）
    "内蒙古自治区", "广西壮族自治区", "西藏自治区", "宁夏回族自治区",
    "新疆维吾尔自治区", "香港特别行政区", "澳门特别行政区",
    "北京市", "上海市", "天津市", "重庆市",
    "黑龙江省", "河北省", "山西省", "辽宁省", "吉林省", "江苏省",
    "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省",
    "湖北省", "湖南省", "广东省", "海南省", "四川省", "贵州省",
    "云南省", "陕西省", "甘肃省", "青海省", "台湾省",
    # 简称（排在全称之后）
    "内蒙古", "广西", "西藏", "宁夏", "新疆", "香港", "澳门",
    "北京", "上海", "天津", "重庆", "黑龙江", "河北", "山西", "辽宁",
    "吉林", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南",
    "湖北", "湖南", "广东", "海南", "四川", "贵州", "云南", "陕西",
    "甘肃", "青海", "台湾",
]

# 常见地级市简称（无"市"后缀时也能识别），通用 xx市 正则兜底
CITY_KNOWN = [
    "广州", "杭州", "南京", "苏州", "深圳", "成都", "武汉", "西安",
    "青岛", "大连", "宁波", "厦门", "长沙", "郑州", "济南", "沈阳",
    "哈尔滨", "佛山", "东莞", "无锡", "常州", "温州", "合肥", "福州",
    "南昌", "南宁", "昆明", "贵阳", "兰州", "太原", "石家庄",
]

MUNICIPALITIES = {"北京", "上海", "天津", "重庆"}

STRUCTURED_KEYS = (
    "country", "province", "city", "district",
    "street", "house_no", "postal_code", "detail",
)

POSTAL_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
CITY_RE = re.compile(r"([一-龥]{2,8}?市)")
DISTRICT_RE = re.compile(r"([一-龥]{2,10}?(?:新区|区|县|旗))")
# 门牌号组合：2号101幢3单元501室 这类连续编号整体提取
HOUSE_RE = re.compile(
    r"(\d+(?:号|幢|栋|号楼|单元|室|楼|层|期)(?:\d+(?:号|幢|栋|号楼|单元|室|楼|层|期))*)"
)
# 道路/街巷级片段，结尾锚定，优先取离门牌号最近的一个；
# 单字"街/道"过于宽泛（如"步行街"），只保留明确的道路通名
ROAD_TAIL_RE = re.compile(
    r"([一-龥A-Za-z0-9]{2,12}?(?:大道|街道|大街|路|巷|弄))"
)


def _take_first(text: str, candidates: List[str]) -> tuple[str, Optional[str]]:
    """从 text 中取第一个命中的候选词（按候选顺序，长名在前），返回剩余文本与命中片段。"""
    for token in candidates:
        idx = text.find(token)
        if idx >= 0:
            return text[:idx] + text[idx + len(token):], text[idx:idx + len(token)]
    return text, None


def _take_regex(text: str, pattern: re.Pattern,
                reject_prefix: str = "") -> tuple[str, Optional[str]]:
    """取第一个命中片段；若命中片段紧接 reject_prefix 中的残留连接字，则跳过继续找。"""
    pos = 0
    while True:
        m = pattern.search(text, pos)
        if not m:
            return text, None
        if reject_prefix and m.start() > 0 and text[m.start() - 1] in reject_prefix:
            pos = m.end()
            continue
        return text[:m.start()] + text[m.end():], m.group(1)


def parse_address(raw: Optional[str]) -> Dict[str, Optional[str]]:
    """把单一地址文本拆成结构化字段，返回包含 parse_status / parse_note 的字典。

    所有字段值均为输入字符串中的逐字片段（直辖市同城复制除外，这是显式规则），
    解析器不校验地名是否真实存在，只做结构识别，避免凭空生成。
    """
    result: Dict[str, Optional[str]] = {k: None for k in STRUCTURED_KEYS}
    result["parse_status"] = "pending"
    result["parse_note"] = None

    original = (raw or "").strip()
    if not original:
        result["parse_note"] = "空地址，无法拆分"
        return result

    work = original

    # 0. 国家（仅在原文出现时记录）
    if "中国" in work:
        result["country"] = "中国"
        work = work.replace("中国", "", 1)

    # 1. 邮编
    work, result["postal_code"] = _take_regex(work, POSTAL_RE)

    # 2. 省级
    work, province = _take_first(work, PROVINCES)
    result["province"] = province

    # 3. 市级：先查已知简称，再通用 xx市（拒绝挂在残留"市"字之后的匹配）
    city: Optional[str] = None
    for token in CITY_KNOWN:
        if token in work:
            city = token
            work = work.replace(token, "", 1)
            break
    if city is None:
        work, city = _take_regex(work, CITY_RE, reject_prefix="市")
    result["city"] = city

    # 直辖市显式规则：省级已含同城信息，市留空会让消费者误判，按同一原文复制
    if result["city"] is None and province:
        short = province.rstrip("市")
        if short in MUNICIPALITIES:
            result["city"] = province

    # 4. 区/县；城市简称被摘走后常残留"市"字并被并入区名（如 广州|市越秀区），显式剥离
    work, district = _take_regex(work, DISTRICT_RE)
    if district and district.startswith("市") and len(district) > 2:
        work = work.replace(district, "市" + district[1:], 1)
        district = district[1:]
    result["district"] = district

    # 5. 门牌号（先编号后道路，保证道路可以结尾锚定）
    work, result["house_no"] = _take_regex(work, HOUSE_RE)

    # 6. 街道/道路：在剩余文本结尾取最近的道路片段
    road_match = None
    matches = list(ROAD_TAIL_RE.finditer(work))
    if matches:
        road_match = matches[-1]
        result["street"] = road_match.group(1)
        work = work[:road_match.start()] + work[road_match.end():]

    # 7. 残余文本作为 detail（仍是原文逐字内容，不做猜测补全）
    remainder = re.sub(r"[，,。.\s]+", " ", work).strip(" -")
    # 清理摘词后残留的单字连接成分（如"中环皇后大道中"被道路匹配后剩下的"中"）
    remainder = re.sub(r"(^|\s)[市省中区县](\s|$)", " ", remainder).strip()
    result["detail"] = remainder or None

    # ---- 状态判定（确定性规则） ------------------------------------------
    extracted = [k for k in STRUCTURED_KEYS if result[k]]
    has_admin = bool(result["province"] or result["city"])
    has_local = bool(result["street"] or result["house_no"])

    if not extracted:
        result["parse_status"] = "pending"
        result["parse_note"] = "未识别到任何结构化片段，保留原文待人工处理"
        return result

    notes: List[str] = []
    if not has_admin:
        notes.append("缺少省/市行政层级")
    if not result["district"]:
        notes.append("缺少区/县")
    if not has_local:
        notes.append("缺少街道/门牌号")

    if has_admin and (has_local or (result["district"] and result["postal_code"])):
        result["parse_status"] = "parsed"
        result["parse_note"] = "；".join(notes) if notes else "完整拆分"
    else:
        result["parse_status"] = "partial"
        result["parse_note"] = "；".join(notes) or "仅拆出部分字段"
    return result


def structured_signature(parsed: Dict[str, Optional[str]]) -> tuple:
    """供等价性核对使用的确定性签名（不含状态元信息之外的易变时间戳）。"""
    return tuple(parsed.get(k) for k in STRUCTURED_KEYS) + (
        parsed.get("parse_status"),
    )

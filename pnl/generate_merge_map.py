"""Generate broker merge map with transitive closure.

Reads merger relationships from docs/broker_mergers.md (hardcoded here)
and resolves transitive chains (e.g., 116W→962H→961K becomes 116W→961K).

Output: data/derived/broker_merge_map.json
Format: {"old_code": "final_active_code", ...}
"""

import json
from pathlib import Path

# Raw mappings: old_code → immediate successor
# Source: docs/broker_mergers.md
RAW_MAPPINGS = {
    # === Section 一: 公司級合併 ===
    "7070": "9366",   # 豐農 → 華南永昌-豐原
    "1380": "9A81",   # 台灣匯立 → 永豐金-匯立
    "5870": "126i",   # 光隆 → 宏遠-光隆
    "5112": "8492",   # 富隆 → 京城-嘉義
    "1520": "1650",   # 瑞士信貸 → 瑞銀

    # === Section 二: 日盛(116x) → 新富邦(961x/962x/963x) ===
    "1160": "961C",   # 日盛 → 南京
    "1161": "961D",   # 日盛-忠孝 → 忠孝
    "1162": "961E",   # 日盛-台南 → 府城
    "1163": "961F",   # 日盛-台中 → 中港→公益
    "1164": "961G",   # 日盛-內湖 → 成功
    "1165": "961H",   # 日盛-板橋 → 新板
    "1166": "962L",   # 日盛-雙和 → 雙和
    "1167": "961J",   # 日盛-嘉義 → 興業
    "1168": "961K",   # 日盛-高雄 → 七賢
    "1169": "961L",   # 日盛-士林 → 士林
    "116A": "961M",   # 日盛-木柵 → 木柵
    "116B": "961N",   # 日盛-中壢 → 西壢→青埔
    "116C": "963A",   # 日盛-八德 → 八德
    "116E": "961Q",   # 日盛-三重 → 重新
    "116F": "961R",   # 日盛-員林 → 南員林
    "116G": "961S",   # 日盛-景美 → 景美
    "116H": "962C",   # 日盛-新竹 → 東門→竹科
    "116J": "961V",   # 日盛-板橋中山 → 板橋中山
    "116K": "961W",   # 日盛-花蓮 → 東花蓮
    "116L": "961X",   # 日盛-大墩 → 大墩
    "116M": "961Y",   # 日盛-屏東 → 屏東
    "116N": "961Z",   # 日盛-永康 → 永康
    "116P": "962A",   # 日盛-信義 → 信義→南港
    "116Q": "962B",   # 日盛-二林 → 二林
    "116S": "962D",   # 日盛-新莊 → 新莊
    "116U": "962F",   # 日盛-桃園 → 北桃園
    "116V": "962G",   # 日盛-斗六 → 斗六
    "116W": "962H",   # 日盛-鳳山 → 鳳山
    "116X": "963B",   # 日盛-宜蘭 → 北宜蘭
    "116Z": "962K",   # 日盛-中和 → 中和
    "116b": "962R",   # 日盛-園區 → 園區
    "116c": "963L",   # 日盛-豐原 → 豐原
    "116d": "962J",   # 日盛-三峽 → 三峽
    "116e": "962U",   # 日盛-竹北 → 縣政
    "116f": "962V",   # 日盛-復興 → 復興
    "116g": "962W",   # 日盛-龍潭 → 龍潭
    "116i": "962E",   # 日盛-樹林 → 樹林
    "116j": "962P",   # 日盛-頭份 → 頭份
    "116k": "962Q",   # 日盛-北高雄 → 北高雄
    "116m": "961P",   # 日盛-和美 → 和美
    "116r": "962X",   # 日盛-文化 → 文化
    "116s": "962Y",   # 日盛-土城 → 土城

    # === Section 二: 新富邦已裁撤 → 既有富邦 ===
    "961D": "9623",   # 忠孝 → 台北
    "961E": "9667",   # 府城 → 台南
    "961G": "9627",   # 成功 → 內湖
    "962L": "9654",   # 雙和 → 永和
    "961J": "9692",   # 興業 → 嘉義
    "961L": "9604",   # 士林 → 陽明
    "961Q": "9677",   # 重新 → 三重
    "961S": "9661",   # 景美 → 新店
    "961V": "9655",   # 板橋中山 → 板橋
    "961W": "9621",   # 東花蓮 → 花蓮
    "961X": "961F",   # 大墩 → 公益
    "962F": "9665",   # 北桃園 → 桃園
    "962H": "961K",   # 鳳山 → 七賢
    "963B": "9686",   # 北宜蘭 → 宜蘭
    "962K": "9654",   # 中和 → 永和
    "962R": "962C",   # 園區 → 竹科
    "962U": "9624",   # 縣政 → 竹北
    "962V": "9663",   # 復興 → 敦南
    "962W": "9636",   # 龍潭 → 中壢
    "962X": "9655",   # 文化 → 板橋
    "962Y": "961H",   # 土城 → 新板

    # === Section 三: 富邦既有分點裁撤 ===
    "9695": "9692",   # 民雄 → 嘉義
    "9651": "9658",   # 民生 → 建國
    "9679": "9676",   # 延吉 → 仁愛
    "9622": "9647",   # 園區 → 新竹

    # === Section 四: 凱基 ===
    "9224": "921S",   # 新莊 → 幸福
    "922H": "920F",   # 復興 → 站前

    # === Section 四: 元富（內部整併） ===
    "592V": "5920",   # 民權 → 元富
    "592d": "5920",   # 延平 → 元富
    "5926": "5920",   # 四維 → 元富
    "592e": "5920",   # 古亭 → 元富

    # === Section 四: 永豐金 ===
    "9A96": "9A00",   # 博愛 → 永豐金
    "9A9E": "9A00",   # 板橋 → 永豐金
    "9A9f": "9A00",   # 中盛 → 永豐金

    # === Section 四: 其他 ===
    "111C": "1113",   # 台灣企銀-三民 → 九如
    "5269": "526M",   # 美好-台中 → 市政
    "9873": "980h",   # 元大-西門 → 台北
    "700E": "700C",   # 兆豐-斗南 → 來福
}


def resolve_transitive(mapping: dict[str, str]) -> dict[str, str]:
    """Resolve transitive chains until all values are final destinations."""
    result = dict(mapping)
    changed = True
    while changed:
        changed = False
        for k in result:
            if result[k] in result:
                result[k] = result[result[k]]
                changed = True
    return result


def main() -> None:
    merge_map = resolve_transitive(RAW_MAPPINGS.copy())

    # Verify no self-loops
    for k, v in merge_map.items():
        assert k != v, f"Self-loop: {k} → {v}"

    # Verify no remaining chains (all targets should NOT be in keys)
    unresolved = [k for k, v in merge_map.items() if v in merge_map]
    assert not unresolved, f"Unresolved chains: {unresolved}"

    # Output
    out_path = Path("data/derived/broker_merge_map.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(merge_map, f, indent=2, ensure_ascii=False)

    print(f"Generated {len(merge_map)} broker mappings → {out_path}")

    # Show some transitive resolutions
    transitive = {
        k: v for k, v in merge_map.items()
        if RAW_MAPPINGS[k] != v
    }
    if transitive:
        print(f"\nTransitive resolutions ({len(transitive)}):")
        for k, v in sorted(transitive.items())[:10]:
            chain = k
            cur = k
            while cur in RAW_MAPPINGS:
                nxt = RAW_MAPPINGS[cur]
                chain += f" → {nxt}"
                cur = nxt
            print(f"  {chain}")


if __name__ == "__main__":
    main()

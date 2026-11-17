#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并多个 cli_*.csv 压测结果"""
import csv
import glob
import os
import sys
from collections import defaultdict

BUCKET_US = 10          # 直方图桶宽：10 微秒
OVER_1S_US = 1_000_000

HEADER = ("latency_buckets,latency_count,latency_over_1s,latency_sum,"
          "max_latency,min_latency,receive_chat_packet,create_chat_packet,"
          "drop_packet,main_running_time,sending_time").split(",")


def load_one(path):
    """解析单个 CSV,返回 dict"""
    with open(path, newline="") as fp:
        rows = [r for r in csv.reader(fp) if r and any(c.strip() for c in r)]
    if len(rows) < 2:
        raise ValueError("行数不足")

    s = rows[1]
    if len(s) < 11:
        raise ValueError(f"summary 字段只有 {len(s)} 个")

    d = {
        "first_bucket": int(s[0]),
        "first_count":  int(s[1]),
        "over_1s":      int(s[2]),
        "latency_sum":  int(s[3]),
        "max_latency":  int(s[4]),
        "min_latency":  int(s[5]),
        "recv_pkt":     int(s[6]),
        "create_pkt":   int(s[7]),
        "drop_pkt":     int(s[8]),
        "main_time":    int(s[9]),
        "send_time":    int(s[10]),
        "buckets":      {},
    }

    # summary 行里带着第一个非零桶
    if d["first_count"] > 0:
        d["buckets"][d["first_bucket"]] = d["first_count"]

    # 后续直方图行
    for r in rows[2:]:
        if len(r) < 2 or not r[0].strip():
            continue
        try:
            d["buckets"][int(r[0])] = int(r[1])
        except ValueError:
            continue
    return d


def percentile(buckets, total, p):
    """返回 p 分位数（微秒），落在 >1s 区域返回 None"""
    if total <= 0:
        return None
    target = total * p / 100.0
    cum = 0
    for idx in sorted(buckets):
        cum += buckets[idx]
        if cum >= target:
            return idx * BUCKET_US      # 返回桶下界
    return None                          # 落在 >1s 区间


def fmt_us(v):
    if v is None:
        return ">1s"
    return f"{v} us ({v/1000.0:.3f} ms)"


def main():
    outdir = sys.argv[1] if len(sys.argv) > 1 else "./stress_test_result"
    files = sorted(glob.glob(os.path.join(outdir, "cli_*.csv")))
    if not files:
        print(f"未找到 {outdir}/cli_*.csv")
        sys.exit(1)

    agg = defaultdict(int)
    buckets = defaultdict(int)
    min_lat = None
    per_client = []
    parsed = 0

    for f in files:
        try:
            d = load_one(f)
        except Exception as e:
            print(f"[WARN] 跳过 {f}: {e}")
            continue

        parsed += 1
        agg["over_1s"]     += d["over_1s"]
        agg["latency_sum"] += d["latency_sum"]
        agg["recv_pkt"]    += d["recv_pkt"]
        agg["create_pkt"]  += d["create_pkt"]
        agg["drop_pkt"]    += d["drop_pkt"]
        agg["max_latency"]  = max(agg["max_latency"], d["max_latency"])

        # min 只在真的收到包时才有意义
        if d["recv_pkt"] > 0:
            min_lat = d["min_latency"] if min_lat is None else min(min_lat, d["min_latency"])

        for k, v in d["buckets"].items():
            buckets[k] += v

        per_client.append((os.path.basename(f), d))

    if parsed == 0:
        print("没有可解析的文件")
        sys.exit(1)

    total_samples = sum(buckets.values()) + agg["over_1s"]
    avg = agg["latency_sum"] / agg["recv_pkt"] if agg["recv_pkt"] else 0
    drop_rate = agg["drop_pkt"] / agg["create_pkt"] * 100 if agg["create_pkt"] else 0

    # ---------- 写合并后的 CSV ----------
    merged_path = os.path.join(outdir, "merged.csv")
    keys = sorted(buckets)
    first = keys[0] if keys else 0
    first_cnt = buckets.get(first, 0)

    with open(merged_path, "w", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(HEADER)
        w.writerow([
            first, first_cnt, agg["over_1s"], agg["latency_sum"],
            agg["max_latency"], min_lat if min_lat is not None else 99999999,
            agg["recv_pkt"], agg["create_pkt"], agg["drop_pkt"],
            max((d["main_time"] for _, d in per_client), default=0),
            max((d["send_time"] for _, d in per_client), default=0),
        ])
        for idx in keys:
            if idx == first:
                continue
            w.writerow([idx, buckets[idx], "", "", "", "", "", "", ""])

    # ---------- 写可读报告 ----------
    lines = []
    lines.append("===== 合并结果 =====")
    lines.append(f"扫描文件: {len(files)} 个，成功解析: {parsed} 个")
    lines.append("-" * 56)
    lines.append(f"接收聊天包 : {agg['recv_pkt']}")
    lines.append(f"创建聊天包 : {agg['create_pkt']}")
    lines.append(f"丢弃包     : {agg['drop_pkt']}  ({drop_rate:.2f}%)")
    lines.append(f"超过 1s 包 : {agg['over_1s']}")
    lines.append(f"平均延迟   : {avg:.2f} us ({avg/1000.0:.3f} ms)")
    lines.append(f"最小延迟   : {fmt_us(min_lat)}")
    lines.append(f"最大延迟   : {fmt_us(agg['max_latency'])}")
    for p in (50, 90, 99, 99.9):
        lines.append(f"P{p:<9}: {fmt_us(percentile(buckets, total_samples, p))}")
    lines.append("-" * 56)
    lines.append("运行时长 main_running_time 各客户端(us): "
                 + str([d["main_time"] for _, d in per_client]))
    lines.append("发送时长 sending_time      各客户端(us): "
                 + str([d["send_time"] for _, d in per_client]))
    lines.append("-" * 56)
    lines.append("逐客户端明细:")
    lines.append(f"{'文件':<16}{'接收':>8}{'创建':>8}{'丢弃':>8}{'平均us':>12}{'P50us':>10}{'P99us':>10}{'最大us':>10}")
    for name, d in per_client:
        n = sum(d["buckets"].values()) + d["over_1s"]
        a = d["latency_sum"] / d["recv_pkt"] if d["recv_pkt"] else 0
        p50 = percentile(d["buckets"], n, 50)
        p99 = percentile(d["buckets"], n, 99)
        lines.append(f"{name:<16}{d['recv_pkt']:>8}{d['create_pkt']:>8}{d['drop_pkt']:>8}"
                     f"{a:>12.2f}{str(p50):>10}{str(p99):>10}{d['max_latency']:>10}")

    report = "\n".join(lines)
    print(report)
    with open(os.path.join(outdir, "merged_report.txt"), "w") as fp:
        fp.write(report + "\n")

    print(f"\n已生成: {merged_path}")
    print(f"已生成: {os.path.join(outdir, 'merged_report.txt')}")


if __name__ == "__main__":
    main()
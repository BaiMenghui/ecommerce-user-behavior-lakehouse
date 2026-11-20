# -*- coding: utf-8 -*-
"""
Kafka 生产端脚本 —— 电商用户行为湖仓分析平台
功能: 读取清洗后的 2017-12-03 当日数据, 按时间顺序重放发送到 Kafka,
      模拟"当日实时用户行为流", 供 Spark Structured Streaming 消费入湖。
数据源: /home/BaiMenghui/hudi-bigdata/data/cleaned/dt=2017-12-03/user_behavior.csv
Topic : user_behavior_stream (单分区)

用法:
    python3 kafka_producer.py                 # 默认 500 条/秒
    python3 kafka_producer.py --speed 1000    # 自定义速率
"""

# ============================================================
# 脚本名称: kafka_producer.py
# 所属阶段: 阶段2 —— 实时链路数据源(模拟实时行为流)
# 功能说明: 读取阶段1清洗产出的 2017-12-03 单日分区 CSV, 按原始顺序
#           重放为 Kafka 消息流(每条行为 = 一条 JSON 消息), 模拟当日
#           实时用户行为流, 供 Spark Structured Streaming 消费入湖;
#           支持 --speed 控制重放速率、--batch 控制每批条数
# 输入    : /home/BaiMenghui/hudi-bigdata/data/cleaned/dt=2017-12-03/user_behavior.csv
# 输出    : Kafka topic user_behavior_stream (localhost:9092)
# 运行方式: wsl bash -c "python3 kafka_producer.py" (可选 --speed 1000)
# ============================================================

# 依赖: argparse 解析速率参数, csv 逐行读取清洗数据, json 序列化消息体,
# time 控制发送节奏; kafka-python 提供生产端客户端
import argparse
import csv
import json
import time

from kafka import KafkaProducer

# Kafka 地址与目标 topic; CSV_PATH 指向阶段1清洗产出的单日分区文件
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "user_behavior_stream"
CSV_PATH = "/home/BaiMenghui/hudi-bigdata/data/cleaned/dt=2017-12-03/user_behavior.csv"


# 构造 KafkaProducer: value 用 JSON 序列化(一条行为一条消息), key 转字符串;
# acks=1 表示 leader 副本落盘即确认, 发送失败自动重试 3 次;
# linger_ms=50 + 64KB 批大小: 消息攒批发送以提升吞吐
def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # JSON 序列化, 一条用户行为消息一条 Kafka 记录
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks=1,
        retries=3,
        # 适当攒批提升吞吐, 与发送速率配合
        linger_ms=50,
        batch_size=64 * 1024,
    )


# 主流程: 解析参数 -> 计算"批间隔"限速节奏 -> 建 producer ->
#         逐行读 CSV 组装 JSON 消息发送 -> flush 收尾并打印对账信息
def main():
    parser = argparse.ArgumentParser(description="用户行为数据 Kafka 生产端(当日流重放)")
    parser.add_argument("--speed", type=int, default=500, help="发送速率(条/秒), 默认 500")
    parser.add_argument("--batch", type=int, default=100, help="每批发送条数, 默认 100")
    args = parser.parse_args()

    # 计算每批发送间隔(秒) = 批条数 / 速率(条/秒): 每发完一批暂停该时长,
    # 使整体重放速率趋近 --speed 指定值, 平滑模拟实时流入
    interval = args.batch / float(args.speed)
    print("==========================================")
    print("Kafka 生产端启动 (当日实时流重放)")
    print("数据源 : %s" % CSV_PATH)
    print("Topic  : %s | 速率: %d 条/秒" % (TOPIC, args.speed))
    print("==========================================")

    producer = build_producer()

    # total 累计发送条数, t0 记录起始时间, 用于打印进度与总耗时
    total = 0
    t0 = time.time()
    # 逐行读取清洗后的 CSV(DictReader 按表头取列), 把字符串字段转回
    # 数值后组装成 JSON 消息 —— 字段与消费端 MESSAGE_SCHEMA 一一对应,
    # 其中 ts 即原始秒级时间戳, dt/event_hour 用于下游分区与时段分析
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            # CSV 列: user_id,item_id,category_id,behavior_type,timestamp,event_time,dt,event_hour
            msg = {
                "user_id": int(row["user_id"]),
                "item_id": int(row["item_id"]),
                "category_id": int(row["category_id"]),
                "behavior_type": row["behavior_type"],
                "ts": int(row["timestamp"]),
                "event_time": row["event_time"],
                "dt": row["dt"],
                "event_hour": int(row["event_hour"]),
            }
            # 以 user_id 作为消息 key, 保证同一用户行为进入同一分区(单分区场景下保持顺序)
            producer.send(TOPIC, key=msg["user_id"], value=msg)
            batch.append(row)
            total += 1

            # 每凑满一批(--batch 条)就 flush 强制推出并按 interval 休眠限速;
            # 每约 1 万条打印一次进度, 便于观察重放速率
            if len(batch) >= args.batch:
                producer.flush()
                time.sleep(interval)
                if total % 10000 < args.batch:
                    print("[%8.1fs] 已发送 %d 条" % (time.time() - t0, total))
                batch = []

    # 收尾: 把最后不足一批的数据推出并关闭 producer 释放连接
    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    # 打印总条数与耗时, 供与消费端入湖条数对账
    print("==========================================")
    print("发送完成: 共 %d 条, 耗时 %.1f 秒" % (total, elapsed))
    print("==========================================")


if __name__ == "__main__":
    main()

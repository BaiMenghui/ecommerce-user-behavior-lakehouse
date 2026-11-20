# 阶段2补充 - Kafka + Structured Streaming 准实时入湖 操作过程汇报

> 电商用户行为湖仓分析平台 —— 本科毕业设计
> 完成日期：2026-10-31
> 定位：在原有"批量全量入湖"链路之上，补充"流式增量入湖"链路，形成**双链路入湖**架构（毕设加分项）

---

## 一、任务目标

| 项目 | 内容 |
|------|------|
| 目标 | 搭建 Kafka + Spark Structured Streaming 准实时入湖链路 |
| 数据 | 清洗后 2017-12-03 当日数据 126,191 条（与批量 ODS 表同源同构） |
| 叙事 | "当日实时流重放"——将 12-03 当天 00:00~16:00 的用户行为按 600 条/秒重放，模拟实时流入湖 |
| 原则 | 不破坏现有批量链路、ADS 五表、SpringBoot API、Vue 大屏 |

## 二、总体架构

```
清洗后数据(CSV, dt=2017-12-03)
        │  kafka_producer.py (600条/秒, JSON, 按user_id作key)
        ▼
Kafka 3.7.1 (KRaft 单节点, topic: user_behavior_stream, 1分区)
        │  Spark Structured Streaming (30秒微批, maxOffsetsPerTrigger=50000)
        ▼
spark-sql-kafka 消费 → from_json 解析 → behavior_id 主键派生
        │  foreachBatch + upsert (MOR, preCombine=ts, partition=dt)
        ▼
Hudi MOR 表 ods_user_behavior_realtime (与批量表 ods_user_behavior_log 同构)
```

**关键设计：与批量链路完全对齐**
- 表结构、主键生成逻辑 `CONCAT(user_id,"_",item_id,"_",ts,"_",behavior_type)`、预合并键 `ts`、分区 `dt` 均与批量 ODS 表一致
- upsert + 预合并键天然幂等：流重放/断点重启不产生重复数据
- 独立表 `ods_user_behavior_realtime`，不污染批量 ODS 表与下游 ADS

## 三、实施过程

### 3.1 环境探查与网络修复
| 检查项 | 结果 |
|--------|------|
| WSL 内存 / 磁盘 | 6.9G 可用 / 951G 可用，充足 |
| Spark | pip 安装的 pyspark 3.3.0（spark-submit 位于 ~/.local/bin） |
| Hudi | hudi-spark3.3-bundle_2.12-0.14.1.jar |
| 清洗数据 | ~/hudi-bigdata/data/cleaned/dt=2017-12-03/user_behavior.csv（12.6 万条） |

**坑 1：WSL DNS 解析失效。** systemd-resolved 链路配置被周期性重置，resolvectl 临时修复会被覆盖。彻底修复：以 root 将 /etc/resolv.conf 从 stub 符号链接替换为静态文件（nameserver 223.5.5.5 / 114.114.114.114）。

### 3.2 Kafka 安装与启动（KRaft 模式，免 ZooKeeper）
- 下载：华为云镜像 kafka_2.13-3.7.1.tgz → 解压至 ~/tools/kafka
- 配置：log.dirs 改为 /home/BaiMenghui/kafka-data（持久化）；JVM 堆 512m（内存让给 Spark）
- 格式化：kafka-storage.sh format（cluster id: GCfoAUxYSbGxl4xDhj9Q6g）
- Topic：user_behavior_stream（1 分区，1 副本）
- 脚本：scripts/start_kafka.sh（幂等：已格式化/已建 topic 自动跳过）、scripts/stop_kafka.sh

**坑 2：wsl 会话退出连带杀掉 -daemon 的 Kafka。** 从 Windows 侧 wsl 命令启动的守护进程，随会话退出被清理（Kafka 无崩溃日志即被杀）。解决：start_kafka.sh 改用 `setsid` 让 broker 脱离会话进程组；验证期间用前台保活方式运行。

### 3.3 目标表与脚本
| 文件（D:\毕设\阶段2-脚本\，已同步 WSL） | 说明 |
|------|------|
| kafka_producer.py | 生产端：读 CSV 按序重放，JSON 序列化，600 条/秒 |
| streaming_to_hudi.py | 消费端：readStream kafka → foreachBatch 写 Hudi upsert |
| stage2b_realtime_table.sql | 建表：ods_user_behavior_realtime（MOR，spark-sql 注册 metastore） |
| run_streaming_ingestion.sh | spark-submit 包装（--packages spark-sql-kafka-0-10_2.12:3.3.0） |
| stage2b_realtime_verification.sql | spark-sql 验证脚本（需在流式任务停止后运行） |
| hudi_realtime_verify.py | DataFrame API 验证脚本（不占 metastore，随时可跑） |

**坑 3：本地 .m2 残留 pom 导致依赖下载失败。** spark-submit --packages 解析 slf4j-api 时命中 ~/.m2 中的 pom-only 残留（只有 pom 没有 jar），Ivy 不再从远端下载直接报 download failed。解决：删除残缺缓存并手动补齐 jar；同时清理 ~/.ivy2/cache/org.slf4j 解析缓存。

**坑 4：Hudi 配置冲突（KeyGenerator）。** spark-sql 建表时 Hudi 默认 SimpleKeyGenerator 并写入 hoodie.properties；流式写入显式传 ComplexKeyGenerator 触发 Config conflict。解决：流式端改为 SimpleKeyGenerator（单主键 + 单分区字段场景完全够用）。

**坑 5：Derby metastore 单连接锁。** 流式任务常驻持有 metastore_db，此时再跑 spark-sql 会 Derby boot 失败。解决：验证改用纯 DataFrame API（读表路径，不连 metastore）；spark-sql 验证需在流式任务停止后运行。

## 四、运行与验证结果

### 4.1 流式运行轨迹
| 微批 | 时间点 | 写入条数 |
|------|--------|---------|
| 1 | 11:12:49 | 12,240 |
| 2 | 11:13:07 | 50,000（maxOffsetsPerTrigger 限流生效） |
| 3 | 11:13:30 | 50,000 |
| 4 | 11:14:00 | 13,951 |

Timeline：4 个 deltacommit 与 4 个微批一一对应（增量 commit 证据）。

### 4.2 三角核对（核心结论）

```
Kafka 发送条数(producer)  = 126,191
Kafka 末尾 offset         = 126,191
消费微批累计(12240+50000+50000+13951) = 126,191
Hudi 表总条数             = 126,191   ← 四方完全一致
```

### 4.3 六项 DataFrame 验证（全部通过）
| 验证项 | 结果 | 结论 |
|--------|------|------|
| 1. 表总条数 | 126,191 | ✓ |
| 2. 分区分布 | dt=2017-12-03 单分区 | ✓ |
| 3. 行为类型分布 | pv 113,054 / cart 7,072 / fav 3,555 / buy 2,510 | ✓ |
| 4. 时间范围 | 2017-12-03 00:00:00 ~ 16:00:06 | ✓ |
| 5. 主键重复 | 0 | ✓ |
| 6. 增量查询（commit3 之后） | 13,951 = 微批 4 条数 | ✓ 增量入湖能力确认 |

## 五、日常操作手册

```bash
# 1. 启动 Kafka（幂等，WSL 内执行）
bash ~/hudi-bigdata/scripts/start_kafka.sh

# 2. 启动流式消费（常驻，Ctrl+C 停止；必须 cd ~/hudi-bigdata）
cd ~/hudi-bigdata && bash scripts/run_streaming_ingestion.sh

# 3. 发送数据（600条/秒，约3.6分钟发完 12.6万条）
python3 ~/hudi-bigdata/scripts/kafka_producer.py            # 默认 500条/秒
python3 ~/hudi-bigdata/scripts/kafka_producer.py --speed 600

# 4. 验证（流式任务运行中也可执行）
cd ~/hudi-bigdata && ~/.local/bin/spark-submit --master local[2] \
    --jars jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    scripts/hudi_realtime_verify.py

# 5. 停止
# 流式任务: 在其终端 Ctrl+C，或 pkill -f streaming_to_hudi.py
bash ~/hudi-bigdata/scripts/stop_kafka.sh   # 停 Kafka（会连带终止流式依赖源）
```

**重复演示特性**：流式 upsert 幂等——producer 再次重放全部数据，Hudi 表条数仍为 126,191 不变（预合并键 ts 去重），可放心反复演示。

## 六、文档与论文口径更新

1. **任务清单**："Kafka+Structured Streaming 准实时入湖（加分项）"一行状态改为"已完成"。
2. **论文建议**（3 处）：
   - 系统架构图：数据接入层增加"Kafka → Structured Streaming"流式通道，与"批量 Spark SQL 入湖"并列，标注"批流双链路"
   - 数据接入章节：补充一小节（半页即可）：KRaft 单节点部署、30 秒微批、maxOffsetsPerTrigger 限流、upsert 幂等写入、Timeline 增量 commit
   - 实验结果：引用三角核对（126,191 条三方一致）与增量查询验证（13,951 条）作为准实时链路正确性证据
3. **答辩话术**：同源数据双链路入湖（批量全量 + 流式增量），表结构/主键/预合并键与批量链路完全对齐，下游分层建模与指标计算可无缝切换数据源；流式 upsert 幂等保证流重放不产生脏数据。

## 七、遗留说明

- Kafka 与流式任务当前均在运行中；关机后按"操作手册"重新拉起即可
- DNS 静态修复在 WSL 重置网络（wsl --shutdown）后可能需重新执行（root 下写回 /etc/resolv.conf）
- 流式表未同步 Hive metastore 之外的元数据；如需 spark-sql 查询流式表，先停流式任务再执行 sql/stage2b_realtime_verification.sql

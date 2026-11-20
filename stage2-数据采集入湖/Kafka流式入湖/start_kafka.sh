#!/bin/bash
# ============================================================
# Kafka(KRaft 单节点) 启动脚本 —— 电商用户行为湖仓分析平台
# 用途: 启动本地 Kafka 3.7.1 (KRaft 模式, 免 ZooKeeper)
#       并创建流式入湖 topic: user_behavior_stream
# 幂等: 重复执行安全 (已格式化不重复格式化, 已建 topic 跳过)
# ============================================================

set -e

KAFKA_HOME=/home/BaiMenghui/tools/kafka
DATA_DIR=/home/BaiMenghui/kafka-data
export JAVA_HOME=/home/BaiMenghui/tools/jdk-17.0.21+9
export PATH=$JAVA_HOME/bin:$PATH
export KAFKA_HEAP_OPTS="-Xmx512m -Xms256m"
TOPIC=user_behavior_stream

echo "=========================================="
echo "Kafka KRaft 单节点启动"
echo "开始时间: $(date "+%Y-%m-%d %H:%M:%S")"
echo "=========================================="

# 步骤1: 调整配置 (持久化数据目录, 避免放在 /tmp)
if ! grep -q "^log.dirs=$DATA_DIR" $KAFKA_HOME/config/kraft/server.properties; then
    sed -i "s|^log.dirs=.*|log.dirs=$DATA_DIR|" $KAFKA_HOME/config/kraft/server.properties
    echo "[1/4] 已设置 log.dirs=$DATA_DIR"
else
    echo "[1/4] log.dirs 配置已就绪"
fi

# 步骤2: 检查进程, 已运行则跳过
if pgrep -f "kafka.Kafka" > /dev/null; then
    echo "[2/4] Kafka 已在运行, 跳过启动"
else
    # 步骤2a: 未格式化则先格式化存储目录
    if [ ! -f "$DATA_DIR/meta.properties" ]; then
        UUID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
        $KAFKA_HOME/bin/kafka-storage.sh format -t $UUID -c $KAFKA_HOME/config/kraft/server.properties
        echo "[2/4] 存储目录已格式化 (cluster id: $UUID)"
    else
        echo "[2/4] 存储目录已格式化, 跳过"
    fi
    # setsid 使 Kafka 脱离当前 shell 会话/进程组:
    # 避免从 Windows 侧 wsl 命令启动时, 会话退出连带杀掉 broker
    setsid $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties
    echo "[2/4] Kafka 启动指令已发出, 等待就绪..."
fi

# 步骤3: 等待 broker 就绪 (最多 60 秒)
READY=0
for i in $(seq 1 30); do
    if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 2
done
if [ $READY -eq 1 ]; then
    echo "[3/4] Kafka broker 就绪 (localhost:9092)"
else
    echo "[3/4] 错误: Kafka 60 秒内未就绪, 请查看 $DATA_DIR/../kafka-logs 或 server.log"
    exit 1
fi

# 步骤4: 创建 topic (幂等)
if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -qx "$TOPIC"; then
    echo "[4/4] topic $TOPIC 已存在, 跳过创建"
else
    $KAFKA_HOME/bin/kafka-topics.sh --create --topic $TOPIC \
        --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1
    echo "[4/4] topic $TOPIC 创建成功 (1 分区)"
fi

echo ""
echo "=========================================="
echo "Kafka 启动完成: $(date "+%Y-%m-%d %H:%M:%S")"
echo "Topic 列表:"
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
echo "=========================================="

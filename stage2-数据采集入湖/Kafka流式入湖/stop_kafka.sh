#!/bin/bash
# ============================================================
# Kafka(KRaft 单节点) 停止脚本 —— 电商用户行为湖仓分析平台
# ============================================================

export JAVA_HOME=/home/BaiMenghui/tools/jdk-17.0.21+9
export PATH=$JAVA_HOME/bin:$PATH

if pgrep -f "kafka.Kafka" > /dev/null; then
    /home/BaiMenghui/tools/kafka/bin/kafka-server-stop.sh
    sleep 3
    if pgrep -f "kafka.Kafka" > /dev/null; then
        echo "Kafka 未能优雅退出, 强制终止..."
        pkill -f "kafka.Kafka"
        sleep 2
    fi
    echo "Kafka 已停止"
else
    echo "Kafka 未在运行"
fi


#!/usr/bin/env bash

# 启动客户端数量，默认 10 个
CLIENTS=${1:-10}

# 创建结果目录
mkdir -p ./stress_test_result

# 清理上一次 Beth 系列测试的结果，避免混入旧数据
rm -f ./stress_test_result/Beth[0-9]*

pids=()

# 并发启动客户端
for i in $(seq 1 "$CLIENTS"); do
    name="Beth$i"

    echo "Starting $name"
    timeout 10s ./stress_client "$name" &

    pids+=("$!")
done

# 等待所有客户端结束
for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
done

echo "All clients finished."
echo "Results are in ./stress_test_result/"

# v3.4.6 + Producer 改造：业务延迟测量支持

## 背景

进入论文实验阶段。需要测吞吐和端到端业务延迟（P50/P95/P99）。当前代码缺两处支持：

1. ScoreResult 缺时间戳字段——无法计算端到端延迟
2. FileToKafkaProducer 限速——无法测 Flink 真实吞吐极限

两处改动相互独立，可分两个 commit 提交。

---

## 改动 1：ScoreResult 加 ingestionTime 和 scoreTime 字段

### 目的

让 analyze.py 的 throughput 模式能计算端到端业务延迟 = scoreTime - ingestionTime。

### 字段语义

- **ingestionTime**：数据被 Kafka 接收的时刻。在 Flink 中用 `ctx.timestamp()` 获取，对应 Kafka record header 里的 timestamp（producer 发送时的本地时间）
- **scoreTime**：LocalProcessor 完成打分时的本地时间，`System.currentTimeMillis()`

两者差值 = 端到端延迟（毫秒）。

### 需要改的文件

#### 文件 1：`src/main/java/com/leejean/data/ScoreResult.java`

加两个字段 + getter/setter + 构造器参数。具体改动：

```java
public class ScoreResult {
    // 现有字段保留
    private long originalSequence;
    private String id;
    private double score;
    private int label;
    private long forestVersion;
    private String phase;
    
    // v3.4.6 新增：用于业务延迟测量
    private long ingestionTime;   // Kafka record timestamp (producer 发送时刻)
    private long scoreTime;       // LocalProcessor 打分完成时刻
    
    // 构造器加参数（保留原有构造器作为兼容，新构造器调用旧的）
    public ScoreResult(long originalSequence, String id, double score, int label,
                       long forestVersion, String phase) {
        // 旧构造器，时间戳字段初始为 0 (兼容)
        this(originalSequence, id, score, label, forestVersion, phase, 0L, 0L);
    }
    
    public ScoreResult(long originalSequence, String id, double score, int label,
                       long forestVersion, String phase,
                       long ingestionTime, long scoreTime) {
        this.originalSequence = originalSequence;
        this.id = id;
        this.score = score;
        this.label = label;
        this.forestVersion = forestVersion;
        this.phase = phase;
        this.ingestionTime = ingestionTime;
        this.scoreTime = scoreTime;
    }
    
    // 加 getter/setter
    public long getIngestionTime() { return ingestionTime; }
    public void setIngestionTime(long ingestionTime) { this.ingestionTime = ingestionTime; }
    public long getScoreTime() { return scoreTime; }
    public void setScoreTime(long scoreTime) { this.scoreTime = scoreTime; }
}
```

#### 文件 2：`src/main/java/com/leejean/flink/LocalProcessorFunction.java`

修改 `buildScoreResult` 方法签名 + 调用点。

**当前签名**（约第 731 行）：

```java
private ScoreResult buildScoreResult(DataPoint dp, double score,
                                     long forestVersion, String phase) {
    return new ScoreResult(
            dp.getOriginalSequence(),
            dp.getId(),
            score,
            dp.getLabel(),
            forestVersion,
            phase
    );
}
```

**新签名**：

```java
private ScoreResult buildScoreResult(DataPoint dp, double score,
                                     long forestVersion, String phase,
                                     ReadOnlyContext ctx) {
    // v3.4.6: 时间戳支持业务延迟分析
    long ingestionTime = ctx.timestamp() != null ? ctx.timestamp() : 0L;
    long scoreTime = System.currentTimeMillis();
    return new ScoreResult(
            dp.getOriginalSequence(),
            dp.getId(),
            score,
            dp.getLabel(),
            forestVersion,
            phase,
            ingestionTime,
            scoreTime
    );
}
```

**所有调用点都要传 ctx 参数**。grep 一下 `buildScoreResult(` 找全部调用点（应该有 7~8 处，分布在 handleStable / handleWarn / handleLocalDriftReported / handleCooldown / handleWaiting / Phase A backlog drain 等），逐个加 `ctx` 参数。

**对于没有 ctx 的调用点**（Phase A backlog drain 在 processBroadcastElement 中调用——那里 ctx 是 Context 不是 ReadOnlyContext，timestamp() 可能不可用），如果出现这种情况：

- 优先用当前 dp 自带的 timestamp（如果 DataPoint 有 originalTimestamp 字段）
- 否则传 0L（向后兼容）
- LOG.debug 一下"missing ingestionTime"以便调试

### 兼容性

旧 ScoreResult 构造器保留——其他代码若直接调用旧构造器不需要改。但 LocalProcessorFunction 的所有 buildScoreResult 调用必须改。

### 验证

修复后跑一次 sudden 数据集：

```bash
# 端到端跑完，看 scores.jsonl 第一行
head -1 scores.jsonl | python3 -c "import json, sys; d=json.loads(sys.stdin.read()); print(d)"
```

期望输出包含：
```
{'seq': 0, 'id': '...', 'score': 0.x, 'label': 0/1, 'forestVersion': 1, 'phase': 'A', 
 'ingestionTime': 1747xxxxxxxxx, 'scoreTime': 1747xxxxxxxxx}
```

两个时间戳都应是约 17 亿（millis since epoch 2026 年的值），scoreTime ≥ ingestionTime。

---

## 改动 2：FileToKafkaProducer 加 --no-rate-limit 参数

### 目的

吞吐测试时让 Producer 不限速，灌满 Kafka，让 Flink 处理速率 = 真实极限。

### 需要改的文件

`src/main/java/com/leejean/main/FileToKafkaProducer.java`（或者类名是别的，找一下）。

### 当前实现

应该有类似这样的代码：

```java
RateLimiter limiter = RateLimiter.create(rate);  // 用 Guava RateLimiter
for (String line : lines) {
    limiter.acquire();  // 限速
    producer.send(record);
}
```

### 修改方案

加一个 CLI 参数 `--no-rate-limit`（或 `--unlimited`），boolean 类型，默认 false：

```java
ParameterTool params = ParameterTool.fromArgs(args);
// ... 现有参数解析 ...
boolean noRateLimit = params.getBoolean("no-rate-limit", false);
int rate = params.getInt("rate", 500);

RateLimiter limiter = null;
if (!noRateLimit) {
    limiter = RateLimiter.create(rate);
    LOG.info("Rate limit: {} records/sec", rate);
} else {
    LOG.info("Rate limit: DISABLED (unlimited)");
}

for (String line : lines) {
    if (limiter != null) {
        limiter.acquire();
    }
    producer.send(record);
}
```

### 验证

带 `--no-rate-limit` 跑一次小数据集（例如 1000 条），观察日志：

```bash
java -jar fa-iforest.jar com.leejean.main.FileToKafkaProducer \
    --bootstrap-servers localhost:9092 \
    --topic source-topic \
    --input shuttle_1000.csv \
    --no-rate-limit
```

期望：
- 启动日志 `Rate limit: DISABLED (unlimited)`
- 1000 条数据 < 1 秒灌完（之前限速 500/秒时要 2 秒）

---

## v3.4.6 范围

**只动两个文件相关的改动**：
1. ScoreResult.java（加字段+构造器）
2. LocalProcessorFunction.java（buildScoreResult 加 ctx 参数）
3. FileToKafkaProducer.java（加 --no-rate-limit 参数）

**不做什么**：
- 不改协议
- 不改算法
- 不改其他类
- 不优化序列化（虽然加了字段后 JSON 会变大几十字节，但与吞吐相比可忽略）

---

## 实施顺序

1. ScoreResult 加字段 → 编译通过 → 提交
2. LocalProcessorFunction.buildScoreResult 改造 → 编译通过 → 提交（注意找全所有调用点）
3. FileToKafkaProducer 加 --no-rate-limit → 编译通过 → 提交
4. mvn package → 端到端跑小测试验证 scores.jsonl 含时间戳

---

## 工作风格提醒

- 三个改动都很手术——不要扩散
- ScoreResult 旧构造器**必须保留**（兼容性）
- 找 buildScoreResult 的全部调用点要彻底（用 grep `buildScoreResult(`）

---

## v3.4 历史回顾

| 版本 | 修复内容 | 状态 |
|---|---|---|
| v3.4 | 联邦投票协议 | ✅ |
| v3.4.1 | DriftReport sink wiring + SLF4J | ✅ |
| v3.4.2 | log4j2 依赖 | ✅ |
| v3.4.3 | Phase A backlog 误消化 | ✅ |
| v3.4.4 | race condition INITIATE 防护 | ✅ |
| v3.4.5 | broadcast state active 清理 | ✅ |
| **v3.4.6** | **业务延迟时间戳 + Producer 不限速** | **本次** |

v3.4.6 是论文实验阶段的支撑性改动——v3.4 协议层面没有变化。

# v2.1 交接文档：mock 全局森林 + Phase A/C 状态机

> 本文是对 `docs/HANDOVER.md` 的增量更新，专注 v2.1 的工作范围。
> v1 已完成（Phase B 训练 + 发树到 Kafka）；本阶段把 LocalProcessor 升级为完整三阶段状态机。

---

## v2.1 范围

**做什么**：
1. 在 `tree/` 下新增 `Forest` 类，实现经典 iForest 打分公式
2. 新增 `ForestMessage` 消息载体（model-topic 上承载的格式）
3. 新增 `ScoreResult` 输出类型（main 输出）
4. **DataPoint 加 `originalSequence` 字段** + CsvToDataPointFunction 加策略参数
5. 把 `LocalTrainerFunction` 升级为 `KeyedBroadcastProcessFunction`（改名为 `LocalProcessorFunction`），实现 Phase A/C 状态机
6. 改造 `LocalProcessor`：增加 model-topic 的 source 流和 broadcast，并 connect 到主流
7. 输出异常分数到 Kafka `output-scores` topic

**不做什么**（明确划线，避免越界）：
- ❌ 不写真协调器作业（v2.2 才做）
- ❌ 不实现 HDDM 漂移检测（v3 才做）
- ❌ 不实现局部漂移检测的 side output（v3 一起做）
- ❌ 不实现广播流到达后重新触发训练（v3 才做）
- ❌ 不实现"从 CSV 字段读 sequence"的策略（保留枚举值即可，throw UnsupportedOperationException）

**测试 mock 路径**：手动用 `kafka-console-producer` 或一个一次性 Java main 类向 `model-topic` 投一条 ForestMessage（v1 跑出来的 100 棵树拼成）。

---

## 已敲定的决策（不要再讨论）

| 项 | 决策 |
|----|------|
| ForestMessage 的 trees 字段 | 直接复用 `List<ITreeMessage>`，不重新设计 |
| Forest 类的位置 | `com.leejean.tree.Forest` |
| 广播 state 的 key | 固定字符串 `"current"`（永远只一份当前森林，新版本覆盖） |
| Phase A 消化策略 | **在 processElement 中消化**（不在 processBroadcastElement） |
| 算子类型 | `KeyedBroadcastProcessFunction<String, DataPoint, ForestMessage, ScoreResult>` |
| originalSequence 来源 | shuttle 数据集用 `PARSE_ID` 策略，AUTO_INCREMENT 作为 fallback，FROM_FIELD 暂不实现 |

---

## ⚠️ 重要：Phase A 消化的实施位置

**所有积压消化逻辑都在 `processElement` 中**，不在 `processBroadcastElement`。

理由：
- `processBroadcastElement` 里访问其他 key 的 keyed state 需要 `applyToKeyedState`，复杂且 key 上下文混乱
- `processElement` 内消化是标准模式，所有 collect 都在正确的 keyed 上下文里
- 积压数据"晚消化几条数据时间"完全可以接受（生产环境数据流持续，几毫秒内就会有新数据触发）

```
processBroadcastElement(forest, ctx, out):
    只做一件事：解析 ForestMessage → Forest，存入 broadcast state（key = "current"）

processElement(point, ctx, out):
    forest = ctx.getBroadcastState().get("current")
    
    if forest == null:
        # Phase B: v1 训练逻辑 + 同时 backlog.add(point)
        return
    
    # forest != null
    
    # Phase A: 该 key 的 backlog 非空 → 先消化（每条 element 触发一次）
    if backlog 非空:
        for dp in backlog:
            out.collect(scoreOf(dp, forest, "A"))
        backlog.clear()
    
    # Phase C: 给当前 element 打分
    out.collect(scoreOf(point, forest, "C"))
```

---

## 关键实现细节

### 1. 修改 DataPoint 类

新增字段：

```java
public class DataPoint {
    // 已有字段保留：id, time, label, features
    private long originalSequence;   // 新增：原始数据顺序号
    
    // getter / setter / @JsonProperty
}
```

`originalSequence` 用于下游分析时按原始顺序排序（实验绘图、计算指标）。

### 2. 修改 CsvToDataPointFunction：策略化的 sequence 提取

新增枚举和策略参数：

```java
public enum SequenceSource {
    PARSE_ID,           // 用 Long.parseLong(id)，要求 id 是数字字符串
    AUTO_INCREMENT,     // 用 AtomicLong 自增（id 不是数字时的 fallback）
    FROM_FIELD          // 从 CSV 的指定列读取（v2.1 抛 UnsupportedOperationException）
}

public class CsvToDataPointFunction ... {
    private final SequenceSource sequenceSource;
    private final transient AtomicLong autoCounter;  // 仅 AUTO_INCREMENT 使用，open() 里初始化
    
    public CsvToDataPointFunction(..., SequenceSource sequenceSource) { 
        this.sequenceSource = sequenceSource; 
    }
    
    @Override
    public void open(Configuration cfg) {
        if (sequenceSource == SequenceSource.AUTO_INCREMENT) {
            this.autoCounter = new AtomicLong(0);
        }
    }
    
    private long extractSequence(DataPoint partial) {
        switch (sequenceSource) {
            case PARSE_ID:
                try {
                    return Long.parseLong(partial.getId());
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                        "PARSE_ID strategy requires numeric id, got: " + partial.getId(), e);
                }
            case AUTO_INCREMENT:
                return autoCounter.getAndIncrement();
            case FROM_FIELD:
                throw new UnsupportedOperationException("FROM_FIELD not implemented in v2.1");
            default:
                throw new IllegalStateException("Unknown SequenceSource: " + sequenceSource);
        }
    }
}
```

LocalProcessor 实例化时使用 `SequenceSource.PARSE_ID`（shuttle id 是 "0", "1", "2", ... 的自增整数字符串）。

**测试**：在 `CsvToDataPointFunctionTest`（如果还没有就新建）补几个 case：
- PARSE_ID 策略下 id="42" → originalSequence=42
- PARSE_ID 策略下 id="abc" → 抛 IllegalStateException
- AUTO_INCREMENT 策略下连续三条 → originalSequence=0, 1, 2
- FROM_FIELD 策略 → 抛 UnsupportedOperationException

### 3. 新增类：`com.leejean.tree.Forest`

经典 iForest 打分公式（Liu et al. 2008 §3）：

```
H(i) = ln(i) + 0.5772156649        // Euler's constant
c(n) = 2*H(n-1) - 2*(n-1)/n   for n > 1
c(1) = 0
c(0) = 0    // by convention

对单点 x:
  对每棵树 t:
    h_t(x) = path_length(x, t)     // 从 root 走到叶子的边数
            + c(leaf.size)          // 叶子样本数的修正项
  E[h(x)] = mean over all trees
  
score(x) = 2 ^ (-E[h(x)] / c(subsampleSize))
```

**接口**：

```java
public class Forest implements Serializable {
    private final List<ITree> trees;
    private final int subsampleSize;
    
    public Forest(List<ITree> trees, int subsampleSize) { ... }
    
    /** 单点打分 / Score a single point. 范围 [0,1]，越大越异常。 */
    public double score(double[] features) { ... }
    
    private double pathLength(double[] features, ITree tree) { ... }
    private static double averagePathLength(int n) { ... }
}
```

**实现注意**：
- `pathLength` 从 root（id=0）开始遍历，叶子时返回 `currentDepth + c(leaf.size)`
- 内部节点：`features[node.feature] < node.threshold` 走左子，否则走右子（与 ITreeBuilder 构建时一致）
- `c(n)` 在 n ≤ 1 时返回 0，避免 `log(0)`
- 用 `Math.log` 自然对数

**测试**：`src/test/java/com/leejean/tree/ForestTest.java`：
- 手动构造一棵已知小树，验证 path length 计算正确
- 用一个明显的"异常点"（远离训练数据），分数 > 0.7
- 用一个"正常点"（训练集中的点），分数 < 0.6
- 100 棵树的 forest 在合成数据上能区分出注入的离群点

### 4. 新增类：`com.leejean.beans.ForestMessage`

```java
public class ForestMessage implements Serializable {
    private String forestId;          // UUID
    private long version;             // 单调递增版本号，v2.1 mock 用 1
    private long createdAt;           // 时间戳
    private int subsampleSize;        // 所有树共用的 ψ
    private List<ITreeMessage> trees; // 直接复用 v1 已有结构
    
    // ... getters/setters with @JsonProperty, no-arg constructor
}
```

**测试**：`src/test/java/com/leejean/beans/ForestMessageTest.java`：JSON 往返一致。

### 5. 新增类：`com.leejean.beans.ScoreResult`

```java
public class ScoreResult implements Serializable {
    private long originalSequence;    // 来自 DataPoint.originalSequence，下游排序用
    private String dataPointId;
    private long timestamp;
    private double[] features;
    private int label;                // 透传 ground truth
    private double score;             // 异常分数 [0,1]
    private long forestVersion;       // 用哪个版本森林打的分
    private String phase;             // "A" 或 "C"，便于实验分析
    
    // ... POJO with @JsonProperty
}
```

### 6. 算子升级：`LocalTrainerFunction` → `LocalProcessorFunction`

**重命名**：删除 `LocalTrainerFunction.java`，新建 `LocalProcessorFunction.java`。
LocalProcessor 里的引用相应更新。

**类签名**：

```java
public class LocalProcessorFunction extends 
    KeyedBroadcastProcessFunction<String, DataPoint, ForestMessage, ScoreResult> {
    
    public static final OutputTag<ITreeMessage> TREE_TAG = 
        new OutputTag<ITreeMessage>("tree-output"){};
    
    public static final MapStateDescriptor<String, Forest> FOREST_DESC = 
        new MapStateDescriptor<>("global-forest", String.class, Forest.class);
}
```

**Keyed State**（per-key）：

```java
ListState<DataPoint> buffer;          // v1 已有
ValueState<Integer> bufferSize;       // v1 已有
ValueState<Integer> treesProduced;    // v1 已有
ListState<DataPoint> backlog;         // 新增：Phase B 期间累积的所有数据
```

**两个回调的伪代码**：

#### `processBroadcastElement(ForestMessage msg, Context ctx, Collector<ScoreResult> out)`

```java
Forest forest = buildForestFromMessage(msg);
ctx.getBroadcastState(FOREST_DESC).put("current", forest);
LOG.info("Subtask {}: received global forest version {} with {} trees", 
         subtaskIndex, msg.getVersion(), msg.getTrees().size());
// 不消化积压，不输出
```

#### `processElement(DataPoint point, ReadOnlyContext ctx, Collector<ScoreResult> out)`

```java
Forest forest = ctx.getBroadcastState(FOREST_DESC).get("current");

if (forest == null) {
    // ===== Phase B: 冷启动 =====
    backlog.add(point);  // 留作未来 Phase A 消化
    
    // v1 训练逻辑（保留）
    Integer produced = treesProduced.value();
    if (produced != null && produced >= localTreeCount) return;
    
    buffer.add(point);
    int sz = (bufferSize.value() == null ? 1 : bufferSize.value() + 1);
    bufferSize.update(sz);
    if (sz < subsampleSize) return;
    
    // 训一棵树并通过 side output 发出
    ITree tree = builder.build(toArray(buffer.get()), subsampleSize);
    ITreeMessage treeMsg = new ITreeMessage(...);
    ctx.output(TREE_TAG, treeMsg);
    
    buffer.clear();
    bufferSize.update(0);
    treesProduced.update(produced == null ? 1 : produced + 1);
    return;
}

// forest != null，进入 Phase A/C

long forestVersion = readCurrentForestVersion(ctx);  // 从 broadcast state 配套读

// ===== Phase A: 消化该 key 的 backlog =====
List<DataPoint> blList = new ArrayList<>();
for (DataPoint dp : backlog.get()) blList.add(dp);
if (!blList.isEmpty()) {
    for (DataPoint dp : blList) {
        double s = forest.score(dp.getFeatures());
        out.collect(buildScoreResult(dp, s, forestVersion, "A"));
    }
    backlog.clear();
}

// ===== Phase C: 当前 element 打分 =====
double s = forest.score(point.getFeatures());
out.collect(buildScoreResult(point, s, forestVersion, "C"));
```

### ⚠️ ListState iterator "一次性"陷阱

`ListState.get()` 在某些 Flink 版本里返回的 Iterable 只能遍历一次。判断"是否非空"和"实际遍历"用同一个 iterable 会出问题。

**正确做法**：先全部 collect 到本地 ArrayList，再判断和遍历（如上面伪代码所示）。或者每次 `state.get()` 重新拿。

### ⚠️ forestVersion 怎么传给 processElement

广播 state 只能存 Forest（key="current"，value=Forest）。version 信息在 ForestMessage 里。两种处理方式：

**方式 1**：把 version 包在 Forest 内部（Forest 加一个 `private long version` 字段）。在 `processBroadcastElement` 里 `new Forest(trees, subsampleSize, version)`。
**方式 2**：单独再加一个 broadcast state entry，key="version"，value=Long。

**推荐方式 1**：简单。Forest 类反正要 Serializable，多一个 long 字段没成本。

### 7. LocalProcessor.java 改造

#### 新增 source（model-topic）

```java
FlinkKafkaConsumer<String> modelConsumer = new FlinkKafkaConsumer<>(
    modelTopic, new SimpleStringSchema(), kafkaProps);
modelConsumer.setStartFromLatest();   // 不消费历史森林（mock 阶段简化）

DataStream<ForestMessage> forestStream = env
    .addSource(modelConsumer)
    .name("Model Source")
    .map(new ForestMessageDeserializer())  // 一个简单的 MapFunction，调 Jackson
    .name("Parse ForestMessage");

BroadcastStream<ForestMessage> broadcastStream = 
    forestStream.broadcast(LocalProcessorFunction.FOREST_DESC);
```

#### 主流 connect

```java
SingleOutputStreamOperator<ScoreResult> processed = keyedStream
    .connect(broadcastStream)
    .process(new LocalProcessorFunction(subsampleSize, localTreeCount, totalTrees, seed))
    .name("Local Processor (Phase A/B/C)");

// 两路输出
DataStream<ITreeMessage> trees = processed.getSideOutput(LocalProcessorFunction.TREE_TAG);
DataStream<ScoreResult> scores = processed;  // 主流：Phase A 和 Phase C 的分数

// Sink to Kafka
trees.addSink(treeProducer);   // 老链路
scores.addSink(scoreProducer); // 新增 → output-scores topic
```

#### 参数新增

```
modelTopic   默认 "model-topic"
scoreTopic   默认 "output-scores"
```

CsvToDataPointFunction 实例化时传 `SequenceSource.PARSE_ID`。

### 8. 测试

#### `LocalProcessorFunctionTest`（MiniCluster 集成测试）

测试三个场景：

**场景 A：纯 Phase B**（无广播输入 → 等同于 v1 测试）
- 喂 25600+ 条数据，不发广播
- 期望：100 棵 tree（side output），0 条 score（main 输出）

**场景 B：Phase B → A → C 切换**
- 先喂 1000 条数据（不够训完 100 棵）
- 然后通过 broadcast input 投一条 ForestMessage（mock 森林，简单的 1-2 棵手工构造的小树即可）
- 再喂 100 条数据
- 期望：
  - tree side output: 看到部分树
  - main score 输出 ≥ 1100 条（积压 1000 + 新数据 100）
  - 通过 phase 字段区分 A 和 C

**场景 C：纯 Phase C**（启动时就有森林）
- 先发 broadcast，再喂 100 条数据
- 期望：所有 100 条数据都走 main score（phase="C"）；backlog 始终为空

测试用的 mock 森林直接用 `Forest` 类构造：1 棵手工小 ITree，subsampleSize=10 即可。

#### 端到端手动测试工具

`src/main/java/com/leejean/tools/MockForestPublisher.java`：

```
独立的 main 函数，从一个本地 JSON 文件读 ForestMessage 投到 Kafka model-topic
用法：MockForestPublisher <kafka-bootstrap> <model-topic> <forest-json-file>
```

JSON 文件准备方式（在 README 里说明）：
1. 跑 v1 一段时间，从 tree-topic 用 kafka-console-consumer 拉够 100 条 ITreeMessage 存到本地
2. 写一个一次性脚本（Java main 或 jq）把 100 条 ITreeMessage 拼成一条 ForestMessage 写到文件
3. 跑 MockForestPublisher 投到 model-topic

---

## 验证步骤（实施完成后逐项检查）

1. `mvn test -Dtest=ITreeBuilderTest` 全绿（v1 不能因为 DataPoint 改动而坏）
2. `mvn test -Dtest=ForestTest` 全绿
3. `mvn test -Dtest=ForestMessageTest` 全绿
4. `mvn test -Dtest=CsvToDataPointFunctionTest` 全绿（含新策略 case）
5. `mvn test -Dtest=LocalProcessorFunctionTest` 三个场景全绿
6. `mvn test`（全部测试）全绿
7. 端到端：起本地 Kafka + Flink，跑 LocalProcessor，能在 output-scores 看到分数 JSON

---

## 工作风格提醒（对应 CLAUDE.md）

- 分提交：每个新类一次提交，状态机改造一次，LocalProcessor 改造一次，测试一次
- 每段完成跑测试再继续
- DataPoint 改动会影响 v1 测试和 ITreeMessage——确认 v1 的 ITreeBuilderTest / ITreeMessageTest 仍然全绿
- 改 LocalProcessor 时**保留** v1 时代的注释，DistributionProbe 那块继续保持注释状态
- ListState iterator 陷阱：必须重新调用 `state.get()` 或先 collect 到 List

---

## 当前接手时第一件事

1. 先跑 `mvn test` 确认 v1 全绿（baseline）
2. 按以下顺序逐步提交：
   1. 修改 DataPoint + CsvToDataPointFunction（加 originalSequence 和 SequenceSource）→ 测试 → 提交
   2. 实现 Forest 类 + 测试 → 提交
   3. 实现 ForestMessage + ScoreResult + 测试 → 提交
   4. 实现 LocalProcessorFunction（删除 LocalTrainerFunction）→ 测试 → 提交
   5. 改造 LocalProcessor.java 接广播流 → 测试 → 提交
   6. 实现 MockForestPublisher 工具 + 写 README 说明 → 提交
3. 每一步报告测试结果再继续

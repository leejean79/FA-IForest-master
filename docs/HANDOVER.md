# FA-iForest 项目交接文档（给 Claude Code）

本文档记录了项目当前的架构决策、第一版范围、以及下一步要做的事。
读完此文档应能在不看历史聊天记录的前提下接手工作。

---

## 1. 项目核心目标

基于 Apache Flink 的流式异常检测系统，使用 iForest 算法。

**核心创新点（区别于已有方案）**：
1. **分布式并行 iForest 构建**：每个 Flink subtask 独立训练本地 iTree，发送到 Kafka，由协调器拼接成全局森林后广播回各 subtask 用于打分。
2. **流式漂移检测**：使用 HDDM（Hoeffding Drift Detection Method）在异常分数序列上做漂移检测，**不依赖窗口批处理**。
3. **漂移检测与全局模型版本解耦**：漂移检测和全局模型更新独立进行。

**测试数据集**：shuttle（9 维特征 + 1 个二分类标签）。

DataPoint 的形态示例：
```
DataPoint{id='1536', time=11:53:54.178, label=0, features=[45.0, 0.0, 89.0, 0.0, 44.0, -20.0, 44.0, 45.0, 2.0]}
```

---

## 2. 整体架构

### 2.1 Local Processor 算子的三阶段状态机

每个 Flink subtask 内部的处理逻辑分三个阶段：

```
Phase B（冷启动 - 当前没有全局模型）:
    缓存到达的所有数据点
    每收集 subsampleSize 条 → 训练 1 棵本地 iTree → 发送到 Kafka tree-topic
    重复 localTreeCount 次后停止训练，继续缓存数据等待全局模型
    本阶段不输出异常分数（globalModel = null）

Phase A（积压消化 - 全局模型刚到达）:
    用 globalModel 给所有缓存的积压数据打分
    输出异常分数到下游
    HDDM 不接收这批积压分数（避免冷启动期假漂移）
    清空缓存后转入 Phase C

Phase C（正常预测）:
    用 globalModel 给每个新到达的点打分
    HDDM 接收每个分数做漂移检测
    本地 iTree 在此阶段不主动训练（等漂移触发再启动新一轮训练）
```

### 2.2 数据流图

```
Kafka: input-data
    ↓ (CSV → DataPoint)
    ↓ keyBy(随机均匀分配)
    ↓
[Processor-0] [Processor-1] [Processor-2] [Processor-3]
    ↓             ↓             ↓             ↓
三路输出（每个 Processor 都产出三种结果）：
  1. main output:  异常分数 → Kafka output-scores
  2. side output:  本地漂移检测结果 → Kafka drift-topic
  3. side output:  本地 iTree → Kafka tree-topic

Kafka: tree-topic
    ↓
[协调器作业 - 第二版才做] 拼接全局森林
    ↓
Kafka: model-topic
    ↓ (广播流)
回到 Processor 算子的 ValueState<Forest>
```

---

## 3. 关键设计决策（已敲定，不再讨论）

### 3.1 算法基底
- **iTree 用经典 iForest 递归随机切分**，不用 Online-Isolation-Forest 的随机投影版本
- 第一版代码追求简单，OIF 的随机投影、unlearn 等留到后续版本

### 3.2 并行度与树数关系
- **总树数（精度参数）**：默认 `totalTrees = 100`（论文推荐值）
- **每个 subtask 训几棵**：`localTreeCount = ceil(totalTrees / parallelism)`
  - 比如 parallelism=4 → localTreeCount = 25
  - 这样保证全局森林总规模不受并行度影响（精度对齐经典 iForest）
- **每棵树的训练样本数**：`subsampleSize = 256`（论文推荐 ψ）
- **树深度上限**：`depthLimit = ceil(log2(subsampleSize)) = 8`

### 3.3 训练策略
- **方案 Y（增量训练）**：每个 subtask 收集 256 条 → 训练 1 棵 → 发出 → 清缓存 → 重复 25 次
- **不**采用方案 X（缓存 6400 条一次性训 25 棵）
- 理由：早送达的树让协调器能更早产出第一版全局模型，缩短冷启动时间

### 3.4 Kafka 消息格式
- iTree 发到 Kafka 时**只发结构化的关键切分信息（JSON）**，不发 Java 对象
- 节点用扁平整数 id 引用（不嵌套），便于 Jackson 序列化
- 叶子节点用 sentinel `-1` 标记（feature/left/right 都是 -1）
- 节点 id 严格前序：父节点 id < 子节点 id

### 3.5 keyBy 策略
- 已实现：`ParallelismKeys` 工具穷举找出每个 subtask 对应的 key
- 每条数据从这组 key 里随机选一个 → 数据均匀分布到所有并行 subtask
- 不按业务字段 keyBy，避免数据倾斜

---

## 4. 分版迭代计划

| 版本 | 范围 | 状态 |
|------|------|------|
| **v1**（当前） | LocalTrainer：Phase B 训练逻辑 + 发送 iTree 到 Kafka | **进行中** |
| **v2** | 协调器作业（拼森林）+ Processor connect 广播流 + Phase A/C 打分 | 未开始 |
| **v3** | HDDM 漂移检测 + 漂移触发新一轮训练 | 未开始 |

第一版**只做 Phase B**——LocalTrainer 算子完成"缓存数据 → 训练 iTree → 发到 Kafka"这一条链路。
**Phase A 和 Phase C 暂不实现**（等 v2 接入广播流后再加）。

---

## 5. v1 第一版的具体范围

### 5.1 已完成（Step 1：iTree 算法层） ✓

提交在 `src/main/java/com/leejean/tree/` 下：

- `ITreeNode.java` - 扁平节点 POJO（含 Jackson 注解）
- `ITree.java` - 树容器（节点列表 + 元信息）
- `ITreeBuilder.java` - 经典 iForest 递归切分算法

测试在 `src/test/java/com/leejean/tree/`：

- `ITreeBuilderTest.java` - 10 个测试用例

**Step 1 验证标准**：`mvn test -Dtest=ITreeBuilderTest` 全绿。

测试覆盖：
1. 元数据正确（subsampleSize=256, dimension=9, depthLimit=8）
2. 根节点 id = 0
3. 叶子样本数之和 = ψ（iForest 不变性）
4. 内部节点的子节点 id 严格 > 父节点 id
5. 实际深度 ≤ depthLimit
6. 固定 seed 可重现
7. 单样本边界（直接是叶子）
8. 全相同样本边界（无法切分，根=叶子）
9. 数据量 > ψ 时正确子采样
10. JSON 序列化往返一致

**接手时修复的问题**：
- `pom.xml`：`maven-surefire-plugin` 默认版本 2.12.4 不支持 JUnit 5，升级到 2.22.2
- `ITreeNode.isLeaf()` 和 `ITree.getRoot()`：被 Jackson 自动序列化为 JSON 属性，反序列化时因无 setter 报 `UnrecognizedPropertyException`，加 `@JsonIgnore` 修复

### 5.2 已完成（Step 2：消息载体） ✓

- `src/main/java/com/leejean/beans/ITreeMessage.java` — 发送到 Kafka tree-topic 的消息结构
  - 字段：`treeId`(UUID)、`producerSubtask`(int)、`createdAt`(long)、`tree`(ITree)
  - Jackson 序列化（`@JsonProperty` + 无参构造）
- `src/test/java/com/leejean/beans/ITreeMessageTest.java` — JSON 序列化往返测试

**Step 2 验证标准**：`mvn test -Dtest=ITreeMessageTest` 全绿。

### 5.3 待完成（Step 3）

#### Step 3：Flink 算子

新建 `src/main/java/com/leejean/flink/LocalTrainerFunction.java`：

`KeyedProcessFunction<String, DataPoint, ITreeMessage>` 实现 v1 的 Phase B 逻辑：

```
Keyed State:
  ListState<DataPoint> buffer        -- 当前累积的训练样本
  ValueState<Integer> treesProduced  -- 本 subtask 已产出的树数

processElement(point, ctx, out):
  if treesProduced >= localTreeCount:
    return  // v1 只训一轮，后续版本会让漂移触发重新训练

  buffer.add(point)
  if buffer.size() == subsampleSize:
    double[][] data = bufferToArray()
    ITree tree = builder.build(data, subsampleSize)
    ITreeMessage msg = wrapAsMessage(tree, subtaskId)
    out.collect(msg)
    buffer.clear()
    treesProduced++
```

参数从 `RuntimeContext.getExecutionConfig().getGlobalJobParameters()` 读取：
- `subsampleSize`（默认 256）
- `localTreeCount`（默认 ceil(totalTrees / parallelism)）
- `seed`（可选，用于可重现）

#### 修改 LocalProcessor.java

- **保留** 现有的 Kafka source、CsvToDataPointFunction、ParallelismKeys、keyBy
- **DistributionProbe + print 那段注释掉但不删**（用户明确选择了方案 3）
- 新增参数解析：`totalTrees`（默认 100）、`subsampleSize`（默认 256）、`treeTopic`（默认 "tree-topic"）
- keyedStream 接 `LocalTrainerFunction` → `FlinkKafkaProducer<ITreeMessage>` 发到 tree-topic

ITreeMessage 序列化到 Kafka：用 Jackson 转 JSON String，再用 SimpleStringSchema 写入。

#### Step 3 测试

`src/test/java/com/leejean/flink/LocalTrainerFunctionTest.java`：

用 Flink MiniCluster 测试。喂 6400 条假数据 + parallelism=4，期望 collector 收到 100 条 ITreeMessage（每个 subtask 25 条）。

**Step 3 验证**：`mvn test -Dtest=LocalTrainerFunctionTest` 全绿。

---

## 6. 工作风格要求（来自 CLAUDE.md）

1. **想清楚再写**：假设要明说，不确定就问，多种方案要列出来
2. **简单优先**：最少代码解决问题，不写预测性的"灵活性"
3. **手术式改动**：只改用户要求的，别顺手"改进"邻近代码
4. **目标驱动**：每一步定义可验证的成功标准（可执行的测试）

代码风格：
- 关键代码加中英文双语注释（项目惯例）
- Java 8 语法
- 文件 UTF-8 编码

工作流：
- 每完成一段（Step 2 / Step 3）→ 跑对应单测验证 → 提交 → 再继续下一段
- 不要一口气把 Step 2 + Step 3 都堆完，分两次提交

---

## 7. 当前接手时的第一件事

1. 检查仓库里是否已经有 `src/main/java/com/leejean/tree/` 这个目录和那 4 个 Step 1 文件
2. 跑 `mvn test -Dtest=ITreeBuilderTest`
3. **如果 Step 1 测试全绿** → 开始 Step 2（ITreeMessage）
4. **如果 Step 1 测试有问题** → 先修 Step 1，告诉用户哪里不对

---

## 8. 已经讨论过、不要再问的问题

- iTree 用经典版还是 OIF 版？→ **经典版**
- localTreeCount 怎么算？→ **ceil(totalTrees / parallelism)**
- 缓存策略？→ **方案 Y（每 256 条训 1 棵，重复 N 次）**
- iTree 发 Kafka 用对象还是 JSON？→ **JSON**
- DistributionProbe 那段保留？→ **注释掉但不删**
- v1 是否实现 Phase A/C？→ **不实现，只做 Phase B**
- v1 是否接广播流？→ **不接，普通 KeyedProcessFunction 即可**
- v1 是否做 HDDM？→ **不做，留到 v3**

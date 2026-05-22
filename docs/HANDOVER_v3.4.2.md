# v3.4.2 修复：日志依赖缺失，logs 目录无生成

## 问题诊断

v3.4.1 修复后跑端到端没有生成 `logs/` 目录——log4j2.properties 没起作用。

代码 review 发现根本原因：

### Bug：pom.xml 缺少 log4j2 实现依赖

`pom.xml` 里只有一个日志相关依赖：

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.30</version>
</dependency>
```

这是 SLF4J 到 **log4j 1.x** 的桥接，但：

1. **没有 log4j 1.x 实现 jar**（缺 `log4j:log4j:1.2.x`）→ SLF4J 找不到日志后端
2. **配置文件是 log4j2 格式**（`log4j2.properties` 用 properties 2.x 语法）→ 即使有 log4j 1.x 也读不懂
3. **没有 log4j2 实现** → 配置文件被完全忽略

后果：所有 `LOG.info(...)` 调用通过 SLF4J 找不到实现，**全部被丢弃**（SLF4J 默认行为）。看到的"控制台输出"实际是 `System.out.println` 和 `forestStream.print()` 的输出，不走日志框架。

---

## 修复方案：统一改用 log4j2

Flink 1.13 官方推荐 log4j2，而且 log4j2.properties 本身就是 log4j2 格式——**应该用 log4j2 实现**，不用 log4j 1.x。

### 修改 pom.xml

**删除**：

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-log4j12</artifactId>
    <version>1.7.30</version>
</dependency>
```

**添加**（建议放在 dependencies 块的末尾位置，保持原结构）：

```xml
<!-- =========================== 日志依赖 / Logging =========================== -->
<!-- v3.4.2：使用 log4j2 实现，与 src/main/resources/log4j2.properties 配套 -->
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>2.17.2</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-api</artifactId>
    <version>2.17.2</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
    <version>2.17.2</version>
</dependency>
```

三个依赖的角色：
- `log4j-slf4j-impl`：SLF4J 绑定到 log4j2（让所有 `org.slf4j.Logger` 调用走 log4j2）
- `log4j-api`：log4j2 公共 API
- `log4j-core`：log4j2 实现（包括读取 properties 文件、写入 appender 等）

### 修改 maven-shade-plugin（必须）

log4j2 用 SPI 机制注册 plugins。打 fat jar 时**必须合并多个 jar 里的 plugin 文件**，否则 log4j2 启动会报 `unable to load plugin`。

在 maven-shade-plugin 的 `<transformers>` 块里**添加**一个 log4j2 专用 transformer：

```xml
<transformers>
    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
        <resource>reference.conf</resource>
    </transformer>
    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
    
    <!-- v3.4.2 新增：合并 log4j2 plugin cache -->
    <transformer implementation="com.github.edwgiz.maven_shade_plugin.log4j2_cache_transformer.PluginsCacheFileTransformer"/>
</transformers>
```

但 `PluginsCacheFileTransformer` 来自一个外部 plugin，需要在 shade plugin 自身的 `<dependencies>` 里加：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.4.1</version>
    
    <!-- v3.4.2 新增：让 shade plugin 加载 log4j2 cache transformer -->
    <dependencies>
        <dependency>
            <groupId>com.github.edwgiz</groupId>
            <artifactId>maven-shade-plugin.log4j2-cachefile-transformer</artifactId>
            <version>2.15</version>
        </dependency>
    </dependencies>
    
    <executions>
        <execution>
            <!-- 原有 execution 不变 -->
            ...
        </execution>
    </executions>
</plugin>
```

### 备选：简化方案（如果上面复杂）

如果 `PluginsCacheFileTransformer` 配置麻烦——**可以暂时不加**，log4j2 在 fat jar 里会打印一行 warning 但仍能工作（启动稍慢，几百毫秒）。**对开发调试足够**。

**保守起见，HANDOVER 推荐先按"完整方案"加 PluginsCacheFileTransformer**。如果 mvn package 报"unknown plugin"或类似错误，再退化到"简化方案"。

---

## 验证

修改后：

1. `mvn clean package` → 检查输出有没有"log4j2 plugin"相关 warning
2. `java -jar target/FA-iForest-*.jar com.leejean.main.LocalProcessor [args...]`
3. 启动后立即检查工作目录：

```bash
ls logs/
# 期望看到：all.log local-processor.log
# 至少看到 all.log 因为 root logger 写它
```

4. 启动 30 秒后，`logs/local-processor.log` 应该有 LocalProcessor 启动配置那一行 INFO 日志

如果**仍然没有 logs 目录**——执行下面的诊断步骤。

---

## 诊断步骤（如果修复后仍无 logs 目录）

### 步骤 1：确认 log4j2 实现已经在 fat jar 里

```bash
jar tf target/FA-iForest-*.jar | grep "log4j-core\|Log4jLogger"
# 期望看到：
# org/apache/logging/log4j/core/LoggerContext.class
# org/apache/logging/slf4j/Log4jLogger.class
```

如果**没有**——pom.xml 改动没生效，重新检查依赖块。

### 步骤 2：确认配置文件在 fat jar 里

```bash
jar tf target/FA-iForest-*.jar | grep log4j2.properties
# 期望看到：log4j2.properties
```

如果没有——`src/main/resources/log4j2.properties` 路径不对，或者被 shade plugin filter 排除了。

### 步骤 3：让 log4j2 输出启动调试信息

启动命令前加一个 JVM 参数：

```bash
java -Dlog4j2.debug=true -jar target/FA-iForest-*.jar ...
```

log4j2 会在启动时打印它**找到的所有配置文件、加载的 appender、绑定的 logger**。仔细看 stderr 输出能定位问题。

### 步骤 4：写权限

确认运行目录有写权限：

```bash
touch logs/test.log && rm logs/test.log
```

如果失败——运行用户没有写权限。

---

## v3.4.2 范围

**做什么**：
1. pom.xml 删除 `slf4j-log4j12` 依赖
2. pom.xml 加 3 个 log4j2 依赖（log4j-slf4j-impl, log4j-api, log4j-core）
3. pom.xml 加 maven-shade-plugin 的 PluginsCacheFileTransformer + 它的 plugin dependency

**不做什么**：
- 不改 log4j2.properties
- 不改任何 Java 代码
- 不改其他依赖版本

---

## 实施顺序

1. 修改 pom.xml（3 处改动：删 slf4j-log4j12 + 加 3 个 log4j2 依赖 + shade transformer + shade plugin dependency）
2. `mvn clean package` 编译——观察有无错误
3. 启动 LocalProcessor 端到端测试
4. 检查 `logs/` 目录是否生成
5. 如果未生成，按"诊断步骤"排查

---

## 工作风格提醒

- 这次只动 **pom.xml 一个文件**——不要顺手改其他东西
- pom.xml 改动后**必须 mvn clean package**，不能用增量编译（依赖变更）
- 如果加 PluginsCacheFileTransformer 报错，先注释掉它跑试试——日志会多一条 warning 但能工作

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FA-iForest — 基于 Apache Flink 的流式异常检测系统，使用 iForest（Isolation Forest）算法对实时数据流进行异常检测。

- **语言**: Java 8
- **构建工具**: Maven
- **核心框架**: Apache Flink 1.13.6 (Scala 2.12)
- **数据源/Sink**: Kafka 2.6.3
- **ML 库**: Smile 2.6.0, JSAT 0.0.9
- **序列化**: Jackson 2.13.5, Flink Avro
- **测试**: JUnit 5 (Jupiter 5.9.3)
- **打包**: maven-shade-plugin（生成 fat jar 用于 Flink 集群部署）

## Build Commands

```bash
# 编译
mvn compile

# 运行测试
mvn test

# 运行单个测试类
mvn test -Dtest=ClassName

# 运行单个测试方法
mvn test -Dtest=ClassName#methodName

# 打包（生成 shaded fat jar）
mvn package

# 清理并打包
mvn clean package
```

## Code Conventions

- 关键代码添加中文和英文双语注释
- Java 开发风格，源码编码 UTF-8
- 包名根路径: `com.leejean`
- 标准 Maven 目录结构: `src/main/java/`, `src/test/java/`, `src/main/resources/`

## Architecture Notes

## Workflow

- 在完成一系列代码后务必进行检查和测试
- 优先运行单个测试验证改动
- 需要生成基于 Maven 工程的 .gitignore 文件


## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. 沟通风格(Response Style)

**一律使用完整、规范的书面表达。** 给出的回答必须是面向他人阅读的成文段落或清单,不再使用简略短语、行话式缩写、或省略主语谓语的口语化短句。每一条结论、每一处改动说明,都需要清晰、明确、可独立理解地呈现给用户,使其无需上下文猜测即可读懂。具体要求:

- 不使用"开干""动手""跑通"等口语化短语作为回答开头;改用完整的动作描述,例如"接下来开始实施这一节的修改"。
- 不使用未经定义的项目内部缩写或行话缩写;首次出现的术语应给出完整名称,必要时附简短解释。
- 表格、代码示例、命令行示例之外的正文部分,必须以完整句子组织,不使用裸名词短语充当一句话。
- 列举要点时,每个条目须为完整的陈述句,而不是名词+省略号的形式。

## 6. 脚本与命令的交付要求(Script Delivery)

**任何在回答中提供的脚本、命令行片段、或可执行的代码块,都必须同时给出明确的使用方式。** 仅给出脚本本身而不说明执行环境与调用方式,会让用户无法直接采用,也无法判断脚本是否符合自身环境。每次提供脚本时,至少必须显式包含以下信息:

1. **执行环境**:脚本运行所在的机器或容器(本地 mac、集群 master 节点、Flink JobManager 容器等),以及需要的工作目录。
2. **调用命令**:用户应当如何启动该脚本,包括完整的命令行示例、必需的参数与可选参数的含义。
3. **前置条件**:脚本依赖的环境变量、文件、服务、或前置步骤(例如"必须先执行 `bash deploy/scripts/3-create-topics.sh` 创建 topic")。
4. **期望产出**:脚本正常执行后,用户应当观察到的现象或产物(例如"在 `analysis-output/` 下生成 `drift-summary.csv`")。
5. **常见失败兜底**:如果存在已知的失败模式或退出码,应明确给出排查方向。

不允许在回答中只丢出一段脚本而不附使用说明,即便该脚本看起来"显而易见"。

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

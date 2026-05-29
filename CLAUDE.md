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

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

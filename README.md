# chroma-java

chroma-java 是一个受 [`chroma-core/chroma`](https://github.com/chroma-core/chroma) 启发、能够嵌入到任意 Java 17 应用中的内存向量数据库实现。该项目以单个 JAR 的形式提供，不依赖任何第三方运行时库，便于直接打包和分发。

## 项目简介与流程

整个项目旨在提供“开箱即用”的 Java 内存向量数据库，所有数据都会保留在 JVM 堆内，进程结束后不会自动持久化。其核心流程可分为两层：快速概览与代码级细节。


### 快速流程概览


1. **创建集合**：通过 `DB.createCollection(name, dimension)` 创建或获取一个指定名称和向量维度的集合。集合由 `VectorCollection` 表示，并使用读写锁保证线程安全。
2. **写入数据**：调用 `VectorCollection.add` 或 `VectorCollection.upsert` 批量写入 ID、向量、可选的文档与元数据。内部会验证维度、复制向量并计算范数，最终以 `VectorRecord` 的形式存储在内存映射中。
3. **查询数据**：通过 `VectorCollection.query` 传入查询向量、`topK` 数量以及可选的元数据过滤条件，集合会对当前快照逐条计算余弦距离并返回最近的若干条记录。
4. **消费结果**：查询结果封装在 `QueryResult` 中，可按需获取 ID、距离、向量、文档和元数据；而按 ID 检索时则会得到 `CollectionResult`。调用方可以使用这些不可变结果对象安全地读取数据。


### 核心代码流程详解

**DB.createCollection**
: 使用 `ConcurrentHashMap.compute` 原子地创建集合，当集合名称已存在时会校验维度是否一致，并直接返回现有实例。这样可以确保同名集合在多线程环境下也只会生成一个 `VectorCollection` 对象。

**VectorCollection.add / upsert**
: 这两个方法最终都会调用内部的 `storeBatch`。在申请写锁之前，`storeBatch` 会逐条检查 ID 是否为空、向量维度是否匹配并复制成新的数组，然后计算 L2 范数并封装为 `VectorRecord`。如果是 `add` 调用，还会在落盘前再次确认没有重复 ID。所有数据最后都会被写入内存中的 `HashMap`，因此需要预留足够的堆内存。

**VectorCollection.query**
: 查询时首先获取读锁并复制一份当前的记录快照，避免长时间阻塞写线程。随后针对每个查询向量计算范数、执行线性扫描，通过优先队列维护距离最近的 `topK` 条记录，并按请求决定是否附带向量、文档或元数据。

**结果封装**
: `CollectionResult` 和 `QueryResult` 都是只读的结果包装器，内部使用不可变集合暴露数据，避免调用方在读取后误改动底层结构。

结合这些步骤，就可以清晰地理解从创建集合、写入数据到执行查询的完整代码路径。


## 功能特性

- 使用 `DB.createCollection` 创建具有固定维度的命名集合。
- 支持批量 `add`、`upsert`、`delete`、`getByIds` 与基于余弦距离的 `query` 操作，可选携带文档与元数据。
- 提供 `MetadataFilter` 组合等值、IN 与数值区间过滤，并通过 `Include` 枚举控制返回字段。
- 集合内部使用读写锁实现线程安全：写操作串行化，读操作可并发执行。
- 纯 Java 17 实现，易于嵌入任意 JVM 应用。

## 构建与测试

仅使用 JDK 即可编译源码并运行轻量级测试程序：

```bash
rm -rf out
javac -d out $(find src/main/java -name "*.java") $(find src/test/java -name "*.java")
java -ea -cp out com.chroma.simpledb.VectorCollectionTest
```

如需打包成包含库与示例入口的单一 JAR，可执行：

```bash
jar --create --file chroma-java.jar -C out .
```

## 使用示例

```java
VectorCollection collection = DB.createCollection("demo", 3);
collection.add(
    List.of("a", "b", "c"),
    List.of(
        new double[]{0.2, 0.1, 0.7},
        new double[]{0.4, 0.4, 0.1},
        new double[]{0.9, 0.1, 0.0}
    ),
    List.of("First doc", "Second doc", "Third doc"),
    List.of(
        Map.of("type", "intro"),
        Map.of("type", "body"),
        Map.of("type", "summary")
    )
);

MetadataFilter filter = MetadataFilter.newBuilder()
    .whereEquals("type", "summary")
    .build();

QueryResult result = collection.query(
    List.of(new double[]{0.8, 0.2, 0.0}),
    2,
    filter,
    EnumSet.of(Include.DOCUMENTS, Include.METADATA)
);

System.out.println(result.getIds());
System.out.println(result.getDistances());
```

上述代码的可运行版本可在 `src/main/java/com/chroma/simpledb/Example.java` 中找到。


```java
MetadataFilter cameraFilter = MetadataFilter.newBuilder()
    .whereIn("cameraNo", List.of("A01", "B06"))
    .whereNumberGreaterThanOrEqual("createdAt", 1700000000000L)
    .whereNumberLessThan("createdAt", 1700600000000L)
    .build();
```

## 主要 API 说明

### `VectorCollection.add` 的参数含义

- `ids`：每条记录的唯一标识，不能为空、不可重复，并与向量列表的顺序一一对应。
- `embeddings`：与 `ids` 对应的向量数组，长度必须与集合创建时声明的维度完全一致。
- `documents`：与每条记录对应的可选文档内容，可为 `null`；若提供则列表长度需与 `ids` 一致。
- `metadatas`：与每条记录对应的可选元数据字典，可为 `null`；若提供则列表长度需与 `ids` 一致。

### `VectorCollection` 公共方法速览

- `getName()`：返回集合的名称，同一名称在进程内指向唯一集合实例。
- `getDimension()`：返回集合固定的向量维度，写入与查询向量必须满足该维度。
- `size()`：返回当前集合持有的记录数量，读操作会在内部加锁以保证一致视图。
- `add(...)`：批量写入新记录，不允许覆盖已有 ID，支持可选的文档与元数据。
- `upsert(...)`：批量写入或覆盖记录，与 `add` 参数相同，若 ID 已存在则会替换原数据。
- `deleteByIds(ids)`：根据 ID 列表删除记录，返回成功删除的条数。
- `getByIds(ids, include)`：按 ID 批量读取记录，并通过 `include` 控制返回的字段，缺失的 ID 会被忽略。

### `MetadataFilter` 常用构造方式

- `whereEquals(key, value)`：要求元数据键的值等于指定对象（可为 `null`）。
- `whereIn(key, candidates)`：要求元数据键的值命中候选列表中的任意一个。
- `whereNumberGreaterThan/GreaterThanOrEqual`：数值字段需大于（或大于等于）指定阈值。
- `whereNumberLessThan/LessThanOrEqual`：数值字段需小于（或小于等于）指定阈值，可与大于条件配合形成闭区间或开区间。


## 设计说明

- 向量以原始数组形式存储，查询时进行线性扫描，实现简单且可以支撑百万级别的嵌入。
- 所有数据均常驻 JVM 内存，不依赖持久化层或外部服务。
- 当检测到向量维度不匹配时，会抛出 `IllegalArgumentException` 以便尽早暴露错误。

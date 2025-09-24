# chroma-java

chroma-java 是一个受 [`chroma-core/chroma`](https://github.com/chroma-core/chroma) 启发、能够嵌入到任意 Java 17 应用中的内存向量数据库实现。该项目以单个 JAR 的形式提供，不依赖任何第三方运行时库，便于直接打包和分发。

## 项目简介与流程

整个库的核心流程如下：

1. **创建集合**：通过 `DB.createCollection(name, dimension)` 创建或获取一个指定名称和向量维度的集合。集合由 `VectorCollection` 表示，并使用读写锁保证线程安全。
2. **写入数据**：调用 `VectorCollection.add` 或 `VectorCollection.upsert` 批量写入 ID、向量、可选的文档与元数据。内部会验证维度、复制向量并计算范数，最终以 `VectorRecord` 的形式存储在内存映射中。
3. **查询数据**：通过 `VectorCollection.query` 传入查询向量、`topK` 数量以及可选的元数据过滤条件，集合会对当前快照逐条计算余弦距离并返回最近的若干条记录。
4. **消费结果**：查询结果封装在 `QueryResult` 中，可按需获取 ID、距离、向量、文档和元数据；而按 ID 检索时则会得到 `CollectionResult`。调用方可以使用这些不可变结果对象安全地读取数据。

通过上述步骤，即可完成从集合创建、数据写入到相似度查询的完整流程。

## 功能特性

- 使用 `DB.createCollection` 创建具有固定维度的命名集合。
- 支持批量 `add`、`upsert`、`delete`、`getByIds` 与基于余弦距离的 `query` 操作，可选携带文档与元数据。
- 提供简单的元数据等值过滤与 `Include` 枚举控制返回字段。
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

QueryResult result = collection.query(
    List.of(new double[]{0.8, 0.2, 0.0}),
    2,
    Map.of("type", "summary"),
    EnumSet.of(Include.DOCUMENTS, Include.METADATA)
);

System.out.println(result.getIds());
System.out.println(result.getDistances());
```

上述代码的可运行版本可在 `src/main/java/com/chroma/simpledb/Example.java` 中找到。

## 设计说明

- 向量以原始数组形式存储，查询时进行线性扫描，实现简单且可以支撑百万级别的嵌入。
- 所有数据均常驻 JVM 内存，不依赖持久化层或外部服务。
- 当检测到向量维度不匹配时，会抛出 `IllegalArgumentException` 以便尽早暴露错误。

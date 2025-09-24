package com.chroma.simpledb;

import java.util.List;
import java.util.Map;

/**
 * Small demo showing how to use the in-memory database.
 * 用一个简短示例演示如何使用内存向量数据库。
 */
public final class Example {

    public static void main(String[] args) {
        // 创建一个维度为 3、名为 demo 的向量集合
        VectorCollection collection = DB.createCollection("demo", 3);

        // 批量写入三条带有文档与元数据的向量记录
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

        // 使用余弦距离查询与查询向量最接近的两个结果，并返回文档与元数据
        QueryResult result = collection.query(
                List.of(new double[]{0.8, 0.2, 0.0}),
                2,
                Map.of("type", "summary"),
                java.util.EnumSet.of(Include.DOCUMENTS, Include.METADATA)
        );

        // 打印查询得到的 ID、距离、文档与元数据
        System.out.println("Query IDs: " + result.getIds());
        System.out.println("Query distances: " + result.getDistances());
        result.getDocuments().ifPresent(docs -> System.out.println("Documents: " + docs));
        result.getMetadatas().ifPresent(metas -> System.out.println("Metadatas: " + metas));
    }

    private Example() {
    }
}

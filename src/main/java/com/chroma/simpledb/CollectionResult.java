package com.chroma.simpledb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 表示按 ID 检索集合时返回的结果。
 * <p>
 * 该结果对象不可变，并根据调用时的 include 参数判断是否实际携带向量、文档或元数据。

 */
public final class CollectionResult {

    private final List<String> ids;
    private final List<double[]> embeddings;
    private final boolean embeddingsIncluded;
    private final List<String> documents;
    private final boolean documentsIncluded;
    private final List<Map<String, Object>> metadatas;
    private final boolean metadatasIncluded;

    CollectionResult(List<String> ids,
                     List<double[]> embeddings,
                     boolean embeddingsIncluded,
                     List<String> documents,
                     boolean documentsIncluded,
                     List<Map<String, Object>> metadatas,
                     boolean metadatasIncluded) {
        this.ids = Collections.unmodifiableList(ids);
        this.embeddings = embeddingsIncluded ? Collections.unmodifiableList(embeddings) : List.of();
        this.embeddingsIncluded = embeddingsIncluded;
        this.documents = documentsIncluded ? Collections.unmodifiableList(documents) : List.of();
        this.documentsIncluded = documentsIncluded;
        this.metadatas = metadatasIncluded ? Collections.unmodifiableList(metadatas) : List.of();
        this.metadatasIncluded = metadatasIncluded;
    }

    /**
     * 返回按输入顺序排列的记录 ID 列表。
     */
    public List<String> getIds() {
        return ids;
    }

    /**
     * 返回可选的向量列表，只有在 include 中请求 EMBEDDINGS 时才会有值。
     */
    public Optional<List<double[]>> getEmbeddings() {
        return embeddingsIncluded ? Optional.of(embeddings) : Optional.empty();
    }

    /**
     * 返回可选的文档列表，只有在 include 中请求 DOCUMENTS 时才会有值。
     */
    public Optional<List<String>> getDocuments() {
        return documentsIncluded ? Optional.of(documents) : Optional.empty();
    }

    /**
     * 返回可选的元数据列表，只有在 include 中请求 METADATA 时才会有值。
     */
    public Optional<List<Map<String, Object>>> getMetadatas() {
        return metadatasIncluded ? Optional.of(metadatas) : Optional.empty();
    }
}

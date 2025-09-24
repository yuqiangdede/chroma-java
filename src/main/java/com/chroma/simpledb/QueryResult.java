package com.chroma.simpledb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of a similarity query.
 * 表示相似度查询返回的结果集合。
 */
public final class QueryResult {

    private final List<List<String>> ids;
    private final List<List<Double>> distances;
    private final List<List<double[]>> embeddings;
    private final boolean embeddingsIncluded;
    private final List<List<String>> documents;
    private final boolean documentsIncluded;
    private final List<List<Map<String, Object>>> metadatas;
    private final boolean metadatasIncluded;

    QueryResult(List<List<String>> ids,
                List<List<Double>> distances,
                List<List<double[]>> embeddings,
                boolean embeddingsIncluded,
                List<List<String>> documents,
                boolean documentsIncluded,
                List<List<Map<String, Object>>> metadatas,
                boolean metadatasIncluded) {
        this.ids = Collections.unmodifiableList(ids);
        this.distances = Collections.unmodifiableList(distances);
        this.embeddings = embeddingsIncluded ? Collections.unmodifiableList(embeddings) : List.of();
        this.embeddingsIncluded = embeddingsIncluded;
        this.documents = documentsIncluded ? Collections.unmodifiableList(documents) : List.of();
        this.documentsIncluded = documentsIncluded;
        this.metadatas = metadatasIncluded ? Collections.unmodifiableList(metadatas) : List.of();
        this.metadatasIncluded = metadatasIncluded;
    }

    /**
     * 返回所有查询的命中 ID，外层列表与查询顺序一一对应。
     */
    public List<List<String>> getIds() {
        return ids;
    }

    /**
     * 返回所有查询的距离结果，距离列表与 {@link #getIds()} 的结构一致。
     */
    public List<List<Double>> getDistances() {
        return distances;
    }

    /**
     * 返回可选的向量结果，只有在 include 中请求 EMBEDDINGS 时才会有值。
     */
    public Optional<List<List<double[]>>> getEmbeddings() {
        return embeddingsIncluded ? Optional.of(embeddings) : Optional.empty();
    }

    /**
     * 返回可选的文档结果，只有在 include 中请求 DOCUMENTS 时才会有值。
     */
    public Optional<List<List<String>>> getDocuments() {
        return documentsIncluded ? Optional.of(documents) : Optional.empty();
    }

    /**
     * 返回可选的元数据结果，只有在 include 中请求 METADATA 时才会有值。
     */
    public Optional<List<List<Map<String, Object>>>> getMetadatas() {
        return metadatasIncluded ? Optional.of(metadatas) : Optional.empty();
    }
}

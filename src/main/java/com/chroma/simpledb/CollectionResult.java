package com.chroma.simpledb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of {@link VectorCollection#getByIds(List, java.util.Set)}.
 * 表示按 ID 检索集合时返回的结果。
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

    public List<String> getIds() {
        return ids;
    }

    public Optional<List<double[]>> getEmbeddings() {
        return embeddingsIncluded ? Optional.of(embeddings) : Optional.empty();
    }

    public Optional<List<String>> getDocuments() {
        return documentsIncluded ? Optional.of(documents) : Optional.empty();
    }

    public Optional<List<Map<String, Object>>> getMetadatas() {
        return metadatasIncluded ? Optional.of(metadatas) : Optional.empty();
    }
}

package com.chroma.simpledb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of a similarity query.
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

    public List<List<String>> getIds() {
        return ids;
    }

    public List<List<Double>> getDistances() {
        return distances;
    }

    public Optional<List<List<double[]>>> getEmbeddings() {
        return embeddingsIncluded ? Optional.of(embeddings) : Optional.empty();
    }

    public Optional<List<List<String>>> getDocuments() {
        return documentsIncluded ? Optional.of(documents) : Optional.empty();
    }

    public Optional<List<List<Map<String, Object>>>> getMetadatas() {
        return metadatasIncluded ? Optional.of(metadatas) : Optional.empty();
    }
}

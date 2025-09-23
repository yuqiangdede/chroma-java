package com.chroma.simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a named collection of dense vectors stored entirely in memory.
 */
public final class VectorCollection {

    private final String name;
    private final int dimension;
    private final Map<String, VectorRecord> records;
    private final ReadWriteLock readWriteLock;

    VectorCollection(String name, int dimension) {
        this.name = name;
        this.dimension = dimension;
        this.records = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public String getName() {
        return name;
    }

    public int getDimension() {
        return dimension;
    }

    public int size() {
        Lock read = readWriteLock.readLock();
        read.lock();
        try {
            return records.size();
        } finally {
            read.unlock();
        }
    }

    public void add(List<String> ids, List<double[]> embeddings) {
        add(ids, embeddings, null, null);
    }

    public void add(List<String> ids,
                    List<double[]> embeddings,
                    List<String> documents,
                    List<Map<String, Object>> metadatas) {
        storeBatch(ids, embeddings, documents, metadatas, false);
    }

    public void upsert(List<String> ids, List<double[]> embeddings) {
        upsert(ids, embeddings, null, null);
    }

    public void upsert(List<String> ids,
                       List<double[]> embeddings,
                       List<String> documents,
                       List<Map<String, Object>> metadatas) {
        storeBatch(ids, embeddings, documents, metadatas, true);
    }

    public int deleteByIds(List<String> ids) {
        Objects.requireNonNull(ids, "ids");
        Lock write = readWriteLock.writeLock();
        write.lock();
        try {
            int removed = 0;
            for (String id : ids) {
                if (records.remove(id) != null) {
                    removed++;
                }
            }
            return removed;
        } finally {
            write.unlock();
        }
    }

    public CollectionResult getByIds(List<String> ids) {
        return getByIds(ids, EnumSet.of(Include.EMBEDDINGS, Include.DOCUMENTS, Include.METADATA));
    }

    public CollectionResult getByIds(List<String> ids, Set<Include> include) {
        Objects.requireNonNull(ids, "ids");
        EnumSet<Include> includes = normalizeInclude(include);
        boolean includeEmbeddings = includes.contains(Include.EMBEDDINGS);
        boolean includeDocuments = includes.contains(Include.DOCUMENTS);
        boolean includeMetadatas = includes.contains(Include.METADATA);

        List<String> resultIds = new ArrayList<>();
        List<double[]> resultEmbeddings = includeEmbeddings ? new ArrayList<>() : null;
        List<String> resultDocuments = includeDocuments ? new ArrayList<>() : null;
        List<Map<String, Object>> resultMetadatas = includeMetadatas ? new ArrayList<>() : null;

        Lock read = readWriteLock.readLock();
        read.lock();
        try {
            for (String id : ids) {
                VectorRecord record = records.get(id);
                if (record == null) {
                    continue;
                }
                resultIds.add(record.id());
                if (includeEmbeddings) {
                    resultEmbeddings.add(Arrays.copyOf(record.embedding(), record.embedding().length));
                }
                if (includeDocuments) {
                    resultDocuments.add(record.document());
                }
                if (includeMetadatas) {
                    resultMetadatas.add(copyMetadataForResult(record.metadata()));
                }
            }
        } finally {
            read.unlock();
        }

        return new CollectionResult(resultIds,
                includeEmbeddings ? resultEmbeddings : List.of(), includeEmbeddings,
                includeDocuments ? resultDocuments : List.of(), includeDocuments,
                includeMetadatas ? resultMetadatas : List.of(), includeMetadatas);
    }

    public QueryResult query(List<double[]> queries, int topK) {
        return query(queries, topK, null, EnumSet.noneOf(Include.class));
    }

    public QueryResult query(List<double[]> queries,
                             int topK,
                             Map<String, Object> whereEq,
                             Set<Include> include) {
        Objects.requireNonNull(queries, "queries");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        for (double[] query : queries) {
            validateVector(query);
        }

        EnumSet<Include> includes = normalizeInclude(include);
        boolean includeEmbeddings = includes.contains(Include.EMBEDDINGS);
        boolean includeDocuments = includes.contains(Include.DOCUMENTS);
        boolean includeMetadatas = includes.contains(Include.METADATA);

        List<List<String>> allIds = new ArrayList<>(queries.size());
        List<List<Double>> allDistances = new ArrayList<>(queries.size());
        List<List<double[]>> allEmbeddings = includeEmbeddings ? new ArrayList<>(queries.size()) : null;
        List<List<String>> allDocuments = includeDocuments ? new ArrayList<>(queries.size()) : null;
        List<List<Map<String, Object>>> allMetadatas = includeMetadatas ? new ArrayList<>(queries.size()) : null;

        Lock read = readWriteLock.readLock();
        read.lock();
        try {
            List<VectorRecord> snapshot = new ArrayList<>(records.values());
            for (double[] query : queries) {
                double queryNorm = l2Norm(query);
                List<ScoredRecord> scored = topKRecords(snapshot, query, queryNorm, topK, whereEq);
                List<String> idsForQuery = new ArrayList<>(scored.size());
                List<Double> distancesForQuery = new ArrayList<>(scored.size());
                List<double[]> embeddingsForQuery = includeEmbeddings ? new ArrayList<>(scored.size()) : null;
                List<String> documentsForQuery = includeDocuments ? new ArrayList<>(scored.size()) : null;
                List<Map<String, Object>> metadatasForQuery = includeMetadatas ? new ArrayList<>(scored.size()) : null;

                for (ScoredRecord scoredRecord : scored) {
                    VectorRecord record = scoredRecord.record;
                    idsForQuery.add(record.id());
                    distancesForQuery.add(scoredRecord.distance);
                    if (includeEmbeddings) {
                        embeddingsForQuery.add(Arrays.copyOf(record.embedding(), record.embedding().length));
                    }
                    if (includeDocuments) {
                        documentsForQuery.add(record.document());
                    }
                    if (includeMetadatas) {
                        metadatasForQuery.add(copyMetadataForResult(record.metadata()));
                    }
                }

                allIds.add(Collections.unmodifiableList(idsForQuery));
                allDistances.add(Collections.unmodifiableList(distancesForQuery));
                if (includeEmbeddings) {
                    allEmbeddings.add(Collections.unmodifiableList(embeddingsForQuery));
                }
                if (includeDocuments) {
                    allDocuments.add(Collections.unmodifiableList(documentsForQuery));
                }
                if (includeMetadatas) {
                    allMetadatas.add(Collections.unmodifiableList(metadatasForQuery));
                }
            }
        } finally {
            read.unlock();
        }

        return new QueryResult(allIds,
                allDistances,
                includeEmbeddings ? allEmbeddings : List.of(), includeEmbeddings,
                includeDocuments ? allDocuments : List.of(), includeDocuments,
                includeMetadatas ? allMetadatas : List.of(), includeMetadatas);
    }

    private void storeBatch(List<String> ids,
                            List<double[]> embeddings,
                            List<String> documents,
                            List<Map<String, Object>> metadatas,
                            boolean allowOverwrite) {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(embeddings, "embeddings");
        if (ids.size() != embeddings.size()) {
            throw new IllegalArgumentException("ids and embeddings must be the same size");
        }
        List<String> normalizedDocuments = normalizeOptionalList(documents, ids.size(), "documents");
        List<Map<String, Object>> normalizedMetadatas = normalizeOptionalList(metadatas, ids.size(), "metadatas");

        List<VectorRecord> newRecords = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("id at index " + i + " must not be null or blank");
            }
            double[] embedding = embeddings.get(i);
            validateVector(embedding);
            double[] embeddingCopy = Arrays.copyOf(embedding, embedding.length);
            double norm = l2Norm(embeddingCopy);
            String document = normalizedDocuments.get(i);
            Map<String, Object> metadata = copyMetadata(normalizedMetadatas.get(i));
            newRecords.add(new VectorRecord(id, embeddingCopy, norm, document, metadata));
        }

        Lock write = readWriteLock.writeLock();
        write.lock();
        try {
            if (!allowOverwrite) {
                for (VectorRecord newRecord : newRecords) {
                    if (records.containsKey(newRecord.id())) {
                        throw new IllegalArgumentException("id already exists: " + newRecord.id());
                    }
                }
            }
            for (VectorRecord newRecord : newRecords) {
                records.put(newRecord.id(), newRecord);
            }
        } finally {
            write.unlock();
        }
    }

    private List<ScoredRecord> topKRecords(List<VectorRecord> snapshot,
                                           double[] query,
                                           double queryNorm,
                                           int topK,
                                           Map<String, Object> whereEq) {
        PriorityQueue<ScoredRecord> heap = new PriorityQueue<>(Comparator.comparingDouble((ScoredRecord s) -> s.distance).reversed());
        for (VectorRecord record : snapshot) {
            if (!record.matchesWhereEq(whereEq)) {
                continue;
            }
            double distance = cosineDistance(query, queryNorm, record);
            if (!Double.isFinite(distance)) {
                continue;
            }
            if (heap.size() < topK) {
                heap.offer(new ScoredRecord(record, distance));
            } else if (distance < heap.peek().distance) {
                heap.poll();
                heap.offer(new ScoredRecord(record, distance));
            }
        }
        List<ScoredRecord> result = new ArrayList<>(heap);
        result.sort(Comparator.comparingDouble(s -> s.distance));
        return result;
    }

    private double cosineDistance(double[] query, double queryNorm, VectorRecord record) {
        double cosine = cosineSimilarity(query, record.embedding(), queryNorm, record.norm());
        double distance = 1.0 - cosine;
        if (distance < 0.0) {
            return 0.0;
        }
        return distance;
    }

    private double cosineSimilarity(double[] a, double[] b, double normA, double normB) {
        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return dot / (normA * normB);
    }

    private double l2Norm(double[] vector) {
        double sum = 0.0;
        for (double v : vector) {
            sum += v * v;
        }
        return Math.sqrt(sum);
    }

    private void validateVector(double[] vector) {
        if (vector == null) {
            throw new IllegalArgumentException("embedding must not be null");
        }
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Expected embedding dimension " + dimension + " but was " + vector.length);
        }
    }

    private <T> List<T> normalizeOptionalList(List<T> values, int expectedSize, String fieldName) {
        if (values == null) {
            List<T> result = new ArrayList<>(expectedSize);
            for (int i = 0; i < expectedSize; i++) {
                result.add(null);
            }
            return result;
        }
        if (values.size() != expectedSize) {
            throw new IllegalArgumentException(fieldName + " must have the same number of elements as ids");
        }
        return values;
    }

    private Map<String, Object> copyMetadata(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        return new java.util.LinkedHashMap<>(metadata);
    }

    private Map<String, Object> copyMetadataForResult(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new java.util.LinkedHashMap<>(metadata));
    }

    private EnumSet<Include> normalizeInclude(Set<Include> include) {
        if (include == null || include.isEmpty()) {
            return EnumSet.noneOf(Include.class);
        }
        return EnumSet.copyOf(include);
    }

    private static final class ScoredRecord {
        final VectorRecord record;
        final double distance;

        ScoredRecord(VectorRecord record, double distance) {
            this.record = record;
            this.distance = distance;
        }
    }
}

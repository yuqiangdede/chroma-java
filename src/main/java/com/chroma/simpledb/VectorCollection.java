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
 * 表示一个全部保存在内存中的命名稠密向量集合。
 */
public final class VectorCollection {

    private final String name;
    private final int dimension;
    private final Map<String, VectorRecord> records;
    private final ReadWriteLock readWriteLock; // 控制并发读写的读写锁

    VectorCollection(String name, int dimension) {
        this.name = name;
        this.dimension = dimension;
        this.records = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock(); // 默认提供公平的读写锁实现
    }

    /**
     * 获取该集合的唯一名称。
     * <p>
     * 名称在整个进程范围内用于区分不同集合，通过 {@link DB#createCollection(String, int)}
     * 得到的同名集合都会指向这一实例。
     *
     * @return 集合名称
     */
    public String getName() {
        return name;
    }

    /**
     * 返回集合在创建时声明的向量维度。
     * <p>
     * 所有写入或查询到该集合的向量都必须严格等于该维度，否则会抛出
     * {@link IllegalArgumentException}。
     *
     * @return 固定的向量维度
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * 获取当前集合中记录的数量。
     * <p>
     * 方法内部加读锁，确保在有并发写入时能够得到一致的数量视图。
     *
     * @return 当前已存储的记录总数
     */
    public int size() {
        Lock read = readWriteLock.readLock();
        read.lock();
        try {
            return records.size();
        } finally {
            read.unlock();
        }
    }

    /**
     * 以最小参数集批量写入多条记录。
     * <p>
     * 与四参版本相比，该方法默认不写入文档和元数据；四参版本会对所有参数做一致性校验。
     * 该写入模式不允许覆盖已有的 ID。
     *
     * @param ids        需要写入的记录 ID，需唯一且与向量列表顺序对应
     * @param embeddings 需要写入的向量数据，长度必须与 {@link #getDimension()} 相同
     */
    public void add(List<String> ids, List<double[]> embeddings) {
        add(ids, embeddings, null, null);
    }

    /**
     * 批量写入记录，不允许覆盖已有 ID。
     * <p>
     * 所有列表参数必须等长：索引 <code>i</code> 处的 ID、向量、文档和元数据共同组成一条记录。
     * 方法会复制向量、计算范数并校验维度，以保证后续查询的正确性。
     *
     * @param ids        记录的唯一标识，不能为空、不可重复
     * @param embeddings 与每个 ID 对应的向量数据，维度固定且不能为空
     * @param documents  与每个 ID 对应的可选文档内容，可传入 {@code null}
     * @param metadatas  与每个 ID 对应的可选元数据字典，可传入 {@code null}
     * @throws IllegalArgumentException 当列表长度不一致、ID 重复或维度不匹配时抛出
     */
    public void add(List<String> ids,
                    List<double[]> embeddings,
                    List<String> documents,
                    List<Map<String, Object>> metadatas) {
        // add 调用最终走向统一的批量写入逻辑，不允许覆盖已有记录
        storeBatch(ids, embeddings, documents, metadatas, false);
    }

    /**
     * 以最小参数集批量写入或更新多条记录。
     *
     * @param ids        需要写入的记录 ID，需与向量一一对应
     * @param embeddings 需要写入的向量数据，长度必须与 {@link #getDimension()} 相同
     */
    public void upsert(List<String> ids, List<double[]> embeddings) {
        upsert(ids, embeddings, null, null);
    }

    /**
     * 批量写入或覆盖已有记录。
     * <p>
     * 当 ID 已存在时，本方法会用提供的新向量、文档与元数据替换原有内容；其余规则与
     * {@link #add(List, List, List, List)} 一致。
     *
     * @param ids        记录的唯一标识，不能为空
     * @param embeddings 新的向量数据，维度必须匹配
     * @param documents  可选的文档内容，为 {@code null} 时表示对应记录没有文档
     * @param metadatas  可选的元数据字典，为 {@code null} 时表示对应记录没有元数据
     * @throws IllegalArgumentException 当输入不满足长度或维度要求时抛出
     */
    public void upsert(List<String> ids,
                    List<double[]> embeddings,
                    List<String> documents,
                    List<Map<String, Object>> metadatas) {
        // upsert 在写入时允许覆盖已有记录
        storeBatch(ids, embeddings, documents, metadatas, true);
    }

    /**
     * 根据 ID 列表删除记录。
     * <p>
     * 方法会加写锁以串行化删除操作，返回成功删除的记录数量。
     *
     * @param ids 需要删除的 ID 列表
     * @return 实际移除的记录条数
     */
    public int deleteByIds(List<String> ids) {
        Objects.requireNonNull(ids, "ids");
        Lock write = readWriteLock.writeLock();
        write.lock();
        try {
            // 写锁保护删除过程，逐个移除匹配的记录
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

    /**
     * 按 ID 批量读取记录，返回完整的嵌入、文档与元数据。
     * <p>
     * 等价于调用 {@link #getByIds(List, Set)}，并默认请求所有可选字段。
     *
     * @param ids 需要查询的 ID 列表
     * @return 按原始顺序返回的查询结果
     */
    public CollectionResult getByIds(List<String> ids) {
        return getByIds(ids, EnumSet.of(Include.EMBEDDINGS, Include.DOCUMENTS, Include.METADATA));
    }

    /**
     * 按 ID 批量读取记录，并根据 include 参数控制返回字段。
     * <p>
     * 结果中只会包含集合里已存在的 ID，缺失的 ID 会被忽略。
     *
     * @param ids     需要查询的 ID 列表
     * @param include 控制返回哪些字段的枚举集合，如未指定则默认只返回 ID
     * @return {@link CollectionResult}，其中的列表均与输入 ID 顺序保持一致
     */
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
            // 逐个查找记录并根据 include 配置构造结果
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

    /**
     * 使用给定的查询向量执行相似度搜索，返回最近的前 {@code topK} 条记录。
     * <p>
     * 等价于调用完整参数的 {@link #query(List, int, Map, Set)} 且不进行元数据过滤，也不额外返回可选字段。
     *
     * @param queries 需要执行搜索的查询向量列表
     * @param topK    每个查询返回的最大结果数，必须为正数
     * @return 查询结果，包含每个查询命中的 ID 与距离
     */
    public QueryResult query(List<double[]> queries, int topK) {
        return query(queries, topK, null, EnumSet.noneOf(Include.class));
    }

    /**
     * 使用给定的查询向量执行相似度搜索，并支持元数据过滤及返回字段控制。
     * <p>
     * 搜索过程中会对所有记录进行线性扫描，采用余弦距离对结果排序。
     *
     * @param queries  需要执行搜索的查询向量列表，元素维度必须匹配集合
     * @param topK     每个查询返回的最大结果数，必须为正
     * @param whereEq  可选的元数据等值过滤条件，只有满足键值完全匹配的记录才会被返回
     * @param include  控制返回哪些字段的枚举集合，例如 {@link Include#DOCUMENTS}
     * @return {@link QueryResult}，包含每个查询对应的命中列表
     * @throws IllegalArgumentException 当 {@code topK} 非正或查询向量维度不匹配时抛出
     */
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
            // 拷贝一份当前数据快照，避免查询期间阻塞写操作
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

        // 预先构建待写入的记录，确保所有校验在获取写锁之前完成
        List<VectorRecord> newRecords = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("id at index " + i + " must not be null or blank");
            }
            double[] embedding = embeddings.get(i);
            validateVector(embedding);
            // 复制向量避免外部修改，并提前计算范数提高查询效率
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
                // 如果不允许覆盖，则先检测是否存在重复 ID
                for (VectorRecord newRecord : newRecords) {
                    if (records.containsKey(newRecord.id())) {
                        throw new IllegalArgumentException("id already exists: " + newRecord.id());
                    }
                }
            }
            // 将记录写入内存映射
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
        // 使用大顶堆保留当前最优的 topK 结果
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
                // 只保留距离更近的候选
                heap.poll();
                heap.offer(new ScoredRecord(record, distance));
            }
        }
        List<ScoredRecord> result = new ArrayList<>(heap);
        result.sort(Comparator.comparingDouble(s -> s.distance));
        return result;
    }

    private double cosineDistance(double[] query, double queryNorm, VectorRecord record) {
        // 将余弦相似度转化为距离（1 - cosine），并避免出现负值
        double cosine = cosineSimilarity(query, record.embedding(), queryNorm, record.norm());
        double distance = 1.0 - cosine;
        if (distance < 0.0) {
            return 0.0;
        }
        return distance;
    }

    private double cosineSimilarity(double[] a, double[] b, double normA, double normB) {
        // 预先计算好的范数为相似度计算提供加速
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
        // 计算向量的 L2 范数，用于后续的余弦相似度
        double sum = 0.0;
        for (double v : vector) {
            sum += v * v;
        }
        return Math.sqrt(sum);
    }

    private void validateVector(double[] vector) {
        // 检查向量对象是否存在以及维度是否满足集合要求
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
            // 如果缺失则补齐为指定长度的 null 列表，便于统一处理
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
        // 保留插入顺序，避免调用方修改原始 map
        return new java.util.LinkedHashMap<>(metadata);
    }

    private Map<String, Object> copyMetadataForResult(Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Map.of();
        }
        // 返回不可变视图，确保结果对象只读
        return Collections.unmodifiableMap(new java.util.LinkedHashMap<>(metadata));
    }

    private EnumSet<Include> normalizeInclude(Set<Include> include) {
        if (include == null || include.isEmpty()) {
            return EnumSet.noneOf(Include.class);
        }
        // 使用 EnumSet 提升 contains 判断效率
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

package com.chroma.simpledb;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 用于表示单条向量记录的不可变数据结构。
 * <p>
 * 每条记录包含 ID、原始向量、预计算的 L2 范数、文档以及元数据快照。
 */
final class VectorRecord {

    private final String id;
    private final double[] embedding;
    private final double norm;
    private final String document;
    private final Map<String, Object> metadata;

    VectorRecord(String id, double[] embedding, double norm, String document, Map<String, Object> metadata) {
        this.id = Objects.requireNonNull(id, "id");
        this.embedding = embedding;
        this.norm = norm;
        this.document = document;
        if (metadata == null || metadata.isEmpty()) {
            this.metadata = Collections.emptyMap();
        } else {
            this.metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
        }
    }

    String id() {
        return id;
    }

    double[] embedding() {
        return embedding;
    }

    double norm() {
        return norm;
    }

    String document() {
        return document;
    }

    Map<String, Object> metadata() {
        return metadata;
    }

    /**
     * 判断当前记录是否满足元数据的等值过滤条件。
     */
    boolean matchesWhereEq(Map<String, Object> whereEq) {
        if (whereEq == null || whereEq.isEmpty()) {
            return true;
        }
        if (metadata.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, Object> entry : whereEq.entrySet()) {
            Object value = metadata.get(entry.getKey());
            if (!Objects.equals(value, entry.getValue())) {
                // 只要存在任一键不匹配，则不满足过滤条件
                return false;
            }
        }
        return true;
    }
}

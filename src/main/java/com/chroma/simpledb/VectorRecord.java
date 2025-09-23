package com.chroma.simpledb;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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
                return false;
            }
        }
        return true;
    }
}

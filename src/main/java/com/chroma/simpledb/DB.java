package com.chroma.simpledb;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Entry point for working with the in-memory vector database.
 */
public final class DB {

    private static final ConcurrentMap<String, VectorCollection> COLLECTIONS = new ConcurrentHashMap<>();

    private DB() {
    }

    /**
     * Create (or retrieve) a collection with the given name and vector dimension.
     *
     * @param name      collection name
     * @param dimension vector dimension (must be &gt; 0)
     * @return the collection instance
     */
    public static VectorCollection createCollection(String name, int dimension) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Collection name must not be blank");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive");
        }

        return COLLECTIONS.compute(name, (key, existing) -> {
            if (existing != null) {
                if (existing.getDimension() != dimension) {
                    throw new IllegalArgumentException(
                            "Collection already exists with different dimension: " + existing.getDimension());
                }
                return existing;
            }
            return new VectorCollection(name, dimension);
        });
    }

    /**
     * Retrieve a collection by name.
     *
     * @param name collection name
     * @return the collection or {@code null} if absent
     */
    public static VectorCollection getCollection(String name) {
        return COLLECTIONS.get(name);
    }

    /**
     * Remove all collections. Intended for tests/demo reset.
     */
    public static void clear() {
        COLLECTIONS.clear();
    }
}

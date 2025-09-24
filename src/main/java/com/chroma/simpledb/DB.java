package com.chroma.simpledb;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 提供操作内存向量数据库的静态入口。
 * <p>
 * 所有集合都由该类统一管理，内部使用线程安全的 {@link ConcurrentHashMap}
 * 按名称缓存 {@link VectorCollection} 实例，便于在不同调用方之间共享。
 */
public final class DB {

    private static final ConcurrentMap<String, VectorCollection> COLLECTIONS = new ConcurrentHashMap<>();

    private DB() {
    }

    /**
     * 根据名称和向量维度创建或获取集合；若集合已存在则直接返回。
     * <p>
     * 当同一名称的集合已经创建时，会验证维度是否保持一致；若维度不同则抛出异常，避免出现混乱的数据结构。
     *
     * @param name      集合名称，不能为空且不允许全是空白字符
     * @param dimension 向量维度，必须为正数
     * @return 对应的集合实例
     */
    public static VectorCollection createCollection(String name, int dimension) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Collection name must not be blank");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive");
        }

        // 使用 ConcurrentHashMap 的 compute 方法原子地创建或返回已有集合
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
     * 根据集合名称获取对应实例；若不存在则返回 {@code null}。
     *
     * @param name collection name 集合名称
     * @return the collection or {@code null} if absent 返回集合或 {@code null}
     */
    public static VectorCollection getCollection(String name) {
        return COLLECTIONS.get(name);
    }

    /**
     * 清空所有集合，主要用于测试或示例复位。
     */
    public static void clear() {
        COLLECTIONS.clear();
    }
}

package com.chroma.simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 用于描述查询时的元数据过滤条件。
 * <p>
 * 支持以下三类约束：
 * <ul>
 *     <li>等值匹配：键对应的值需要与指定对象完全一致；</li>
 *     <li>IN 匹配：键对应的值必须命中给定列表中的任意一个候选值；</li>
 *     <li>数值区间：键对应的值需要在设定的最小/最大阈值范围内（用于时间戳等数值字段）。</li>
 * </ul>
 * 使用 {@link Builder} 可以直观地组合上述条件；若三类约束都为空，则视为不过滤。
 */
public final class MetadataFilter {

    private static final MetadataFilter EMPTY = new MetadataFilter(Map.of(), Map.of(), Map.of());

    private final Map<String, Object> equalsConditions;
    private final Map<String, List<Object>> inConditions;
    private final Map<String, NumberRange> numberRanges;

    private MetadataFilter(Map<String, Object> equalsConditions,
                           Map<String, List<Object>> inConditions,
                           Map<String, NumberRange> numberRanges) {
        this.equalsConditions = equalsConditions;
        this.inConditions = inConditions;
        this.numberRanges = numberRanges;
    }

    /**
     * 创建一个空过滤器，等价于不设置任何元数据条件。
     */
    public static MetadataFilter empty() {
        return EMPTY;
    }

    /**
     * 仅使用等值匹配初始化过滤器，供保持旧版 API 行为时使用。
     */
    static MetadataFilter fromEquals(Map<String, Object> equalsConditions) {
        if (equalsConditions == null || equalsConditions.isEmpty()) {
            return empty();
        }
        Map<String, Object> copy = new LinkedHashMap<>(equalsConditions);
        return new MetadataFilter(Collections.unmodifiableMap(copy), Map.of(), Map.of());
    }

    /**
     * 当且仅当三类条件均为空时返回 {@code true}。
     */
    boolean isEmpty() {
        return equalsConditions.isEmpty() && inConditions.isEmpty() && numberRanges.isEmpty();
    }

    Map<String, Object> equalsConditions() {
        return equalsConditions;
    }

    Map<String, List<Object>> inConditions() {
        return inConditions;
    }

    Map<String, NumberRange> numberRanges() {
        return numberRanges;
    }

    /**
     * 创建一个新的过滤器构造器实例。
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * 描述单个数值字段的范围约束。
     */
    static final class NumberRange {
        final Double min;
        final boolean minInclusive;
        final Double max;
        final boolean maxInclusive;

        NumberRange(Double min, boolean minInclusive, Double max, boolean maxInclusive) {
            this.min = min;
            this.minInclusive = minInclusive;
            this.max = max;
            this.maxInclusive = maxInclusive;
        }
    }

    /**
     * 负责逐步拼装 {@link MetadataFilter} 的构造器。
     */
    public static final class Builder {

        private final Map<String, Object> equalsConditions = new LinkedHashMap<>();
        private final Map<String, List<Object>> inConditions = new LinkedHashMap<>();
        private final Map<String, NumberRange> numberRanges = new LinkedHashMap<>();

        private Builder() {
        }

        /**
         * 指定某个键必须等于给定的值（可为 {@code null}）。
         */
        public Builder whereEquals(String key, Object value) {
            validateKey(key);
            equalsConditions.put(key, value);
            return this;
        }

        /**
         * 指定某个键的值必须命中候选列表中的任意一个。
         */
        public Builder whereIn(String key, Collection<?> candidates) {
            validateKey(key);
            Objects.requireNonNull(candidates, "candidates");
            if (candidates.isEmpty()) {
                throw new IllegalArgumentException("candidates must not be empty");
            }
            List<Object> values = new ArrayList<>(candidates.size());
            values.addAll(candidates);
            inConditions.put(key, Collections.unmodifiableList(values));
            return this;
        }

        /**
         * 指定某个数值字段需要大于给定阈值（不包含）。
         */
        public Builder whereNumberGreaterThan(String key, Number minExclusive) {
            return applyMin(key, minExclusive, false);
        }

        /**
         * 指定某个数值字段需要大于等于给定阈值（包含）。
         */
        public Builder whereNumberGreaterThanOrEqual(String key, Number minInclusive) {
            return applyMin(key, minInclusive, true);
        }

        /**
         * 指定某个数值字段需要小于给定阈值（不包含）。
         */
        public Builder whereNumberLessThan(String key, Number maxExclusive) {
            return applyMax(key, maxExclusive, false);
        }

        /**
         * 指定某个数值字段需要小于等于给定阈值（包含）。
         */
        public Builder whereNumberLessThanOrEqual(String key, Number maxInclusive) {
            return applyMax(key, maxInclusive, true);
        }

        /**
         * 最终构造出不可变的 {@link MetadataFilter}。
         */
        public MetadataFilter build() {
            if (equalsConditions.isEmpty() && inConditions.isEmpty() && numberRanges.isEmpty()) {
                return MetadataFilter.empty();
            }
            Map<String, Object> equalsCopy = equalsConditions.isEmpty()
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(equalsConditions));
            Map<String, List<Object>> inCopy;
            if (inConditions.isEmpty()) {
                inCopy = Map.of();
            } else {
                Map<String, List<Object>> tmp = new LinkedHashMap<>();
                for (Map.Entry<String, List<Object>> entry : inConditions.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        throw new IllegalStateException("candidates must not be empty");
                    }
                    tmp.put(entry.getKey(), entry.getValue());
                }
                inCopy = Collections.unmodifiableMap(tmp);
            }
            Map<String, NumberRange> rangeCopy;
            if (numberRanges.isEmpty()) {
                rangeCopy = Map.of();
            } else {
                Map<String, NumberRange> tmp = new LinkedHashMap<>();
                for (Map.Entry<String, NumberRange> entry : numberRanges.entrySet()) {
                    NumberRange range = entry.getValue();
                    if (range == null) {
                        continue;
                    }
                    validateRange(entry.getKey(), range);
                    tmp.put(entry.getKey(), range);
                }
                rangeCopy = Collections.unmodifiableMap(tmp);
            }
            return new MetadataFilter(equalsCopy, inCopy, rangeCopy);
        }

        private Builder applyMin(String key, Number min, boolean inclusive) {
            validateKey(key);
            Objects.requireNonNull(min, "min");
            double value = min.doubleValue();
            NumberRange current = numberRanges.get(key);
            boolean maxInclusive = current != null && current.max != null && current.maxInclusive;
            NumberRange updated = new NumberRange(value, inclusive, current == null ? null : current.max, maxInclusive);
            numberRanges.put(key, updated);
            return this;
        }

        private Builder applyMax(String key, Number max, boolean inclusive) {
            validateKey(key);
            Objects.requireNonNull(max, "max");
            double value = max.doubleValue();
            NumberRange current = numberRanges.get(key);
            boolean minInclusive = current != null && current.min != null && current.minInclusive;
            NumberRange updated = new NumberRange(current == null ? null : current.min, minInclusive, value, inclusive);
            numberRanges.put(key, updated);
            return this;
        }

        private void validateKey(String key) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("metadata key must not be null or blank");
            }
        }

        private void validateRange(String key, NumberRange range) {
            if (range.min != null && range.max != null) {
                int cmp = Double.compare(range.min, range.max);
                if (cmp > 0) {
                    throw new IllegalStateException("min must not be greater than max for key: " + key);
                }
                if (cmp == 0 && (!range.minInclusive || !range.maxInclusive)) {
                    throw new IllegalStateException("exclusive bounds collapse range for key: " + key);
                }
            }
        }
    }
}

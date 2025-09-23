package com.chroma.simpledb;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Lightweight test harness without external dependencies.
 */
public final class VectorCollectionTest {

    public static void main(String[] args) {
        VectorCollectionTest test = new VectorCollectionTest();
        test.addAndQueryReturnsNearest();
        test.metadataFilterLimitsResults();
        test.dimensionMismatchThrows();
        test.upsertReplacesExisting();
        test.deleteRemovesEntries();
        System.out.println("All VectorCollection tests passed.");
    }

    private void addAndQueryReturnsNearest() {
        DB.clear();
        VectorCollection collection = DB.createCollection("test", 3);
        collection.add(
                List.of("a", "b", "c"),
                List.of(
                        new double[]{0.2, 0.1, 0.7},
                        new double[]{0.4, 0.4, 0.1},
                        new double[]{0.9, 0.1, 0.0}
                ),
                List.of("doc-a", "doc-b", "doc-c"),
                List.of(
                        Map.of("kind", "intro"),
                        Map.of("kind", "body"),
                        Map.of("kind", "summary")
                )
        );

        QueryResult result = collection.query(
                List.of(new double[]{0.85, 0.15, 0.05}),
                2,
                Map.of(),
                EnumSet.of(Include.EMBEDDINGS, Include.DOCUMENTS)
        );

        assertEquals(1, result.getIds().size(), "Expected a single query batch");
        assertEquals(List.of("c", "b"), result.getIds().get(0), "Unexpected query order");
        List<Double> distances = result.getDistances().get(0);
        assertEquals(2, distances.size(), "Unexpected number of distances");
        assertTrue(distances.get(0) <= distances.get(1) + 1e-9, "Distances are not sorted ascending");
        assertTrue(result.getEmbeddings().isPresent(), "Embeddings should be included");
        assertEquals(2, result.getEmbeddings().get().get(0).size(), "Embedding count mismatch");
        assertTrue(result.getDocuments().isPresent(), "Documents should be included");
        assertEquals(List.of("doc-c", "doc-b"), result.getDocuments().get().get(0), "Document order mismatch");
    }

    private void metadataFilterLimitsResults() {
        DB.clear();
        VectorCollection collection = DB.createCollection("filter", 2);
        collection.add(
                List.of("x", "y"),
                List.of(
                        new double[]{1.0, 0.0},
                        new double[]{0.0, 1.0}
                ),
                List.of("doc-x", "doc-y"),
                List.of(
                        Map.of("tag", "keep"),
                        Map.of("tag", "drop")
                )
        );

        QueryResult filtered = collection.query(
                List.of(new double[]{0.9, 0.1}),
                2,
                Map.of("tag", "keep"),
                EnumSet.noneOf(Include.class)
        );

        assertEquals(List.of(List.of("x")), filtered.getIds(), "Metadata filter should only return matching id");
        assertEquals(1, filtered.getDistances().get(0).size(), "Unexpected number of distances after filtering");
    }

    private void dimensionMismatchThrows() {
        DB.clear();
        VectorCollection collection = DB.createCollection("dimension", 4);
        boolean addFailed = false;
        try {
            collection.add(List.of("bad"), List.of(new double[]{1.0, 2.0, 3.0}));
        } catch (IllegalArgumentException expected) {
            addFailed = true;
        }
        assertTrue(addFailed, "Adding with wrong dimension should fail");

        boolean queryFailed = false;
        try {
            collection.query(List.of(new double[]{1.0, 2.0, 3.0}), 1);
        } catch (IllegalArgumentException expected) {
            queryFailed = true;
        }
        assertTrue(queryFailed, "Querying with wrong dimension should fail");
    }

    private void upsertReplacesExisting() {
        DB.clear();
        VectorCollection collection = DB.createCollection("upsert", 2);
        collection.add(
                List.of("id"),
                List.of(new double[]{0.1, 0.9}),
                List.of("first"),
                List.of(Map.of("v", 1))
        );

        collection.upsert(
                List.of("id"),
                List.of(new double[]{0.9, 0.1}),
                List.of("second"),
                List.of(Map.of("v", 2))
        );

        CollectionResult result = collection.getByIds(List.of("id"));
        assertEquals(List.of("id"), result.getIds(), "Missing id after upsert");
        assertTrue(result.getDocuments().isPresent(), "Documents should be present");
        assertEquals(List.of("second"), result.getDocuments().get(), "Document should be updated");
        assertTrue(result.getMetadatas().isPresent(), "Metadata should be present");
        assertEquals(2, result.getMetadatas().get().get(0).get("v"), "Metadata should be updated");
    }

    private void deleteRemovesEntries() {
        DB.clear();
        VectorCollection collection = DB.createCollection("delete", 2);
        collection.add(
                List.of("a", "b"),
                List.of(new double[]{1.0, 0.0}, new double[]{0.0, 1.0})
        );

        assertEquals(2, collection.size(), "Initial size mismatch");
        int removed = collection.deleteByIds(List.of("a"));
        assertEquals(1, removed, "Expected to remove a single item");
        assertEquals(1, collection.size(), "Size should decrease after deletion");
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}

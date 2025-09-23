package com.chroma.simpledb;

import java.util.List;
import java.util.Map;

/**
 * Small demo showing how to use the in-memory database.
 */
public final class Example {

    public static void main(String[] args) {
        VectorCollection collection = DB.createCollection("demo", 3);

        collection.add(
                List.of("a", "b", "c"),
                List.of(
                        new double[]{0.2, 0.1, 0.7},
                        new double[]{0.4, 0.4, 0.1},
                        new double[]{0.9, 0.1, 0.0}
                ),
                List.of("First doc", "Second doc", "Third doc"),
                List.of(
                        Map.of("type", "intro"),
                        Map.of("type", "body"),
                        Map.of("type", "summary")
                )
        );

        QueryResult result = collection.query(
                List.of(new double[]{0.8, 0.2, 0.0}),
                2,
                Map.of("type", "summary"),
                java.util.EnumSet.of(Include.DOCUMENTS, Include.METADATA)
        );

        System.out.println("Query IDs: " + result.getIds());
        System.out.println("Query distances: " + result.getDistances());
        result.getDocuments().ifPresent(docs -> System.out.println("Documents: " + docs));
        result.getMetadatas().ifPresent(metas -> System.out.println("Metadatas: " + metas));
    }

    private Example() {
    }
}

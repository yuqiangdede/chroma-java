# chroma-java

A minimal, embeddable in-memory vector database for Java 17 inspired by the core
usage of [`chroma-core/chroma`](https://github.com/chroma-core/chroma). The
implementation keeps everything in memory and is delivered as a single JAR with
no runtime dependencies beyond the JDK.

## Features

- Create named collections with a fixed vector dimension using `DB.createCollection`.
- Batch `add`, `upsert`, `delete`, `getByIds`, and cosine-similarity `query`
  operations with optional documents and metadata.
- Cosine distance (1 - cosine similarity) search with simple metadata equality
  filtering and controllable result payload (`Include` flags).
- Thread-safe collections: writes are serialized, reads can happen concurrently.
- Pure Java 17 implementation suitable for embedding in any JVM-based
  application.

## Building & Testing

Compile the sources and run the lightweight test harness using only the JDK:

```bash
rm -rf out
javac -d out $(find src/main/java -name "*.java") $(find src/test/java -name "*.java")
java -ea -cp out com.chroma.simpledb.VectorCollectionTest
```

To produce a single JAR containing the library and example entry point:

```bash
jar --create --file chroma-java.jar -C out .
```

## Usage Example

```java
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
    EnumSet.of(Include.DOCUMENTS, Include.METADATA)
);

System.out.println(result.getIds());
System.out.println(result.getDistances());
```

A runnable version of the snippet above is available in
`src/main/java/com/chroma/simpledb/Example.java`.

## Design Notes

- Vectors are stored in their raw form and scanned linearly for queries to keep
  the implementation simple while still supporting million-scale embeddings.
- All data lives in the JVM process—there is no persistence layer or external
  service dependency.
- Dimension mismatches are rejected with an `IllegalArgumentException` to
  surface errors early.

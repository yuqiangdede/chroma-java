package com.chroma.simpledb;

/**
 * Controls which optional fields are returned from read operations.
 * 控制读取操作返回的可选字段。
 */
public enum Include {
    EMBEDDINGS,
    DOCUMENTS,
    METADATA
}

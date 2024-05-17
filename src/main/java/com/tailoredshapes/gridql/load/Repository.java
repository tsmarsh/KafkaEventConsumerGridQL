package com.tailoredshapes.gridql.load;

import java.util.concurrent.CompletableFuture;

public interface Repository<I, T> {
    CompletableFuture<String> create(T payload);

    CompletableFuture<String> update(I id, T payload);

    CompletableFuture<String> delete(I id);
}

package com.tailoredshapes.gridql.load;

import com.tailoredshapes.stash.Stash;

import com.tailoredshapes.underbar.io.Requests;

import java.util.concurrent.CompletableFuture;

import static com.tailoredshapes.underbar.io.Requests.*;
import static java.util.function.Function.identity;

public class ReSTRepository implements Repository<String, Stash> {
    private final String baseUrl;

    public ReSTRepository(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public CompletableFuture<String> create(Stash payload) {
        return post(baseUrl, payload.toJSONString(), identity());
    }

    @Override
    public CompletableFuture<String> update(String id, Stash payload) {
        return put("%s/%s".formatted(baseUrl, id), payload.toJSONString(), identity());
    }

    @Override
    public CompletableFuture<String> delete(String id) {
        return Requests.delete("%s/%s".formatted(baseUrl, id), identity());
    }
}

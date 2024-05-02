package com.tailoredshapes.gridql.kafka;

import com.tailoredshapes.stash.Stash;

public interface Repository<I, T> {
    void create(T payload);

    void update(I id, T payload);

    void delete(I id);
}

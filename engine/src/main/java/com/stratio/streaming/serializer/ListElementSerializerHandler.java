package com.stratio.streaming.serializer;

import java.util.ArrayList;
import java.util.List;

public abstract class ListElementSerializerHandler<A, B> implements Serializer<A, B> {

    private static final long serialVersionUID = 6732911270924811360L;

    @Override
    public List<B> serialize(List<A> object) {
        List<B> result = new ArrayList<>();
        if (object != null) {
            for (A aType : object) {
                result.add(serialize(aType));
            }
        }

        return result;
    }

    @Override
    public List<A> deserialize(List<B> object) {
        List<A> result = new ArrayList<>();
        if (object != null) {
            for (B message : object) {
                result.add(deserialize(message));
            }
        }
        return result;
    }
}

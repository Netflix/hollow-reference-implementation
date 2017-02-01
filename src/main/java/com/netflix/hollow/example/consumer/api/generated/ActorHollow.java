package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class ActorHollow extends HollowObject {

    public ActorHollow(ActorDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int _getActorId() {
        return delegate().getActorId(ordinal);
    }

    public Integer _getActorIdBoxed() {
        return delegate().getActorIdBoxed(ordinal);
    }

    public StringHollow _getActorName() {
        int refOrdinal = delegate().getActorNameOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getStringHollow(refOrdinal);
    }

    public MovieAPI api() {
        return typeApi().getAPI();
    }

    public ActorTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected ActorDelegate delegate() {
        return (ActorDelegate)delegate;
    }

}
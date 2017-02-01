package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class MovieHollow extends HollowObject {

    public MovieHollow(MovieDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int _getId() {
        return delegate().getId(ordinal);
    }

    public Integer _getIdBoxed() {
        return delegate().getIdBoxed(ordinal);
    }

    public StringHollow _getTitle() {
        int refOrdinal = delegate().getTitleOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getStringHollow(refOrdinal);
    }

    public SetOfActorHollow _getActors() {
        int refOrdinal = delegate().getActorsOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getSetOfActorHollow(refOrdinal);
    }

    public MovieAPI api() {
        return typeApi().getAPI();
    }

    public MovieTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected MovieDelegate delegate() {
        return (MovieDelegate)delegate;
    }

}
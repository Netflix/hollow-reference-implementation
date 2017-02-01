package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class MovieTypeAPI extends HollowObjectTypeAPI {

    private final MovieDelegateLookupImpl delegateLookupImpl;

    MovieTypeAPI(MovieAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "id",
            "title",
            "actors"
        });
        this.delegateLookupImpl = new MovieDelegateLookupImpl(this);
    }

    public int getId(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleInt("Movie", ordinal, "id");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
    }

    public Integer getIdBoxed(int ordinal) {
        int i;
        if(fieldIndex[0] == -1) {
            i = missingDataHandler().handleInt("Movie", ordinal, "id");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public int getTitleOrdinal(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleReferencedOrdinal("Movie", ordinal, "title");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[1]);
    }

    public StringTypeAPI getTitleTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public int getActorsOrdinal(int ordinal) {
        if(fieldIndex[2] == -1)
            return missingDataHandler().handleReferencedOrdinal("Movie", ordinal, "actors");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[2]);
    }

    public SetOfActorTypeAPI getActorsTypeAPI() {
        return getAPI().getSetOfActorTypeAPI();
    }

    public MovieDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public MovieAPI getAPI() {
        return (MovieAPI) api;
    }

}
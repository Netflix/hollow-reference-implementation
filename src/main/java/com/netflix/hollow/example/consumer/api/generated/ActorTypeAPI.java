package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class ActorTypeAPI extends HollowObjectTypeAPI {

    private final ActorDelegateLookupImpl delegateLookupImpl;

    ActorTypeAPI(MovieAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "actorId",
            "actorName"
        });
        this.delegateLookupImpl = new ActorDelegateLookupImpl(this);
    }

    public int getActorId(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleInt("Actor", ordinal, "actorId");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
    }

    public Integer getActorIdBoxed(int ordinal) {
        int i;
        if(fieldIndex[0] == -1) {
            i = missingDataHandler().handleInt("Actor", ordinal, "actorId");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public int getActorNameOrdinal(int ordinal) {
        if(fieldIndex[1] == -1)
            return missingDataHandler().handleReferencedOrdinal("Actor", ordinal, "actorName");
        return getTypeDataAccess().readOrdinal(ordinal, fieldIndex[1]);
    }

    public StringTypeAPI getActorNameTypeAPI() {
        return getAPI().getStringTypeAPI();
    }

    public ActorDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public MovieAPI getAPI() {
        return (MovieAPI) api;
    }

}
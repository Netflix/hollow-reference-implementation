package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class ActorDelegateLookupImpl extends HollowObjectAbstractDelegate implements ActorDelegate {

    private final ActorTypeAPI typeAPI;

    public ActorDelegateLookupImpl(ActorTypeAPI typeAPI) {
        this.typeAPI = typeAPI;
    }

    public int getActorId(int ordinal) {
        return typeAPI.getActorId(ordinal);
    }

    public Integer getActorIdBoxed(int ordinal) {
        return typeAPI.getActorIdBoxed(ordinal);
    }

    public int getActorNameOrdinal(int ordinal) {
        return typeAPI.getActorNameOrdinal(ordinal);
    }

    public ActorTypeAPI getTypeAPI() {
        return typeAPI;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

}
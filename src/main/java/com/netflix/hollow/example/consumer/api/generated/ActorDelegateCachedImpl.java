package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class ActorDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, ActorDelegate {

    private final Integer actorId;
    private final int actorNameOrdinal;
   private ActorTypeAPI typeAPI;

    public ActorDelegateCachedImpl(ActorTypeAPI typeAPI, int ordinal) {
        this.actorId = typeAPI.getActorIdBoxed(ordinal);
        this.actorNameOrdinal = typeAPI.getActorNameOrdinal(ordinal);
        this.typeAPI = typeAPI;
    }

    public int getActorId(int ordinal) {
        return actorId.intValue();
    }

    public Integer getActorIdBoxed(int ordinal) {
        return actorId;
    }

    public int getActorNameOrdinal(int ordinal) {
        return actorNameOrdinal;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public ActorTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (ActorTypeAPI) typeAPI;
    }

}
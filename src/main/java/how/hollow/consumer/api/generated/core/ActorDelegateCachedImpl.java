package how.hollow.consumer.api.generated.core;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class ActorDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, ActorDelegate {

    private final Integer actorId;
    private final String actorName;
    private final int actorNameOrdinal;
    private ActorTypeAPI typeAPI;

    public ActorDelegateCachedImpl(ActorTypeAPI typeAPI, int ordinal) {
        this.actorId = typeAPI.getActorIdBoxed(ordinal);
        this.actorNameOrdinal = typeAPI.getActorNameOrdinal(ordinal);
        int actorNameTempOrdinal = actorNameOrdinal;
        this.actorName = actorNameTempOrdinal == -1 ? null : typeAPI.getAPI().getStringTypeAPI().getValue(actorNameTempOrdinal);
        this.typeAPI = typeAPI;
    }

    public int getActorId(int ordinal) {
        if(actorId == null)
            return Integer.MIN_VALUE;
        return actorId.intValue();
    }

    public Integer getActorIdBoxed(int ordinal) {
        return actorId;
    }

    public String getActorName(int ordinal) {
        return actorName;
    }

    public boolean isActorNameEqual(int ordinal, String testValue) {
        if(testValue == null)
            return actorName == null;
        return testValue.equals(actorName);
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
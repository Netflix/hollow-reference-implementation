package how.hollow.consumer.api.generated.core;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

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

    public String getActorName(int ordinal) {
        ordinal = typeAPI.getActorNameOrdinal(ordinal);
        return ordinal == -1 ? null : typeAPI.getAPI().getStringTypeAPI().getValue(ordinal);
    }

    public boolean isActorNameEqual(int ordinal, String testValue) {
        ordinal = typeAPI.getActorNameOrdinal(ordinal);
        return ordinal == -1 ? testValue == null : typeAPI.getAPI().getStringTypeAPI().isValueEqual(ordinal, testValue);
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
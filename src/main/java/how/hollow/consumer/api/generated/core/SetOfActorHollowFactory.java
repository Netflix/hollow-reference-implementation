package how.hollow.consumer.api.generated.core;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.Actor;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.objects.provider.HollowFactory;
import com.netflix.hollow.core.read.dataaccess.HollowTypeDataAccess;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowSetCachedDelegate;

@SuppressWarnings("all")
public class SetOfActorHollowFactory<T extends SetOfActor> extends HollowFactory<T> {

    @Override
    public T newHollowObject(HollowTypeDataAccess dataAccess, HollowTypeAPI typeAPI, int ordinal) {
        return (T)new SetOfActor(((SetOfActorTypeAPI)typeAPI).getDelegateLookupImpl(), ordinal);
    }

    @Override
    public T newCachedHollowObject(HollowTypeDataAccess dataAccess, HollowTypeAPI typeAPI, int ordinal) {
        return (T)new SetOfActor(new HollowSetCachedDelegate((SetOfActorTypeAPI)typeAPI, ordinal), ordinal);
    }

}
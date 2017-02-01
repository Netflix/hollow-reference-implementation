package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.HollowSet;
import com.netflix.hollow.core.schema.HollowSetSchema;
import com.netflix.hollow.api.objects.delegate.HollowSetDelegate;
import com.netflix.hollow.api.objects.generic.GenericHollowRecordHelper;

@SuppressWarnings("all")
public class SetOfActorHollow extends HollowSet<ActorHollow> {

    public SetOfActorHollow(HollowSetDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    @Override
    public ActorHollow instantiateElement(int ordinal) {
        return (ActorHollow) api().getActorHollow(ordinal);
    }

    @Override
    public boolean equalsElement(int elementOrdinal, Object testObject) {
        return GenericHollowRecordHelper.equalObject(getSchema().getElementType(), elementOrdinal, testObject);
    }

    public MovieAPI api() {
        return typeApi().getAPI();
    }

    public SetOfActorTypeAPI typeApi() {
        return (SetOfActorTypeAPI) delegate.getTypeAPI();
    }

}
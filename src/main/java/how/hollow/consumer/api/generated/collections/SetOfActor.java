package how.hollow.consumer.api.generated.collections;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.Actor;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.objects.HollowSet;
import com.netflix.hollow.core.schema.HollowSetSchema;
import com.netflix.hollow.api.objects.delegate.HollowSetDelegate;
import com.netflix.hollow.api.objects.generic.GenericHollowRecordHelper;

@SuppressWarnings("all")
public class SetOfActor extends HollowSet<Actor> {

    public SetOfActor(HollowSetDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    @Override
    public Actor instantiateElement(int ordinal) {
        return (Actor) api().getActor(ordinal);
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
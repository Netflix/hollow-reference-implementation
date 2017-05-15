package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class Actor extends HollowObject {

    public Actor(ActorDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int getActorId() {
        return delegate().getActorId(ordinal);
    }

    public Integer getActorIdBoxed() {
        return delegate().getActorIdBoxed(ordinal);
    }

    public HString getActorName() {
        int refOrdinal = delegate().getActorNameOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getHString(refOrdinal);
    }

    public MovieAPI api() {
        return typeApi().getAPI();
    }

    public ActorTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected ActorDelegate delegate() {
        return (ActorDelegate)delegate;
    }

}
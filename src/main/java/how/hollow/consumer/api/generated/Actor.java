package how.hollow.consumer.api.generated;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.UniqueKeyIndex;
import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.tools.stringifier.HollowRecordStringifier;

@SuppressWarnings("all")
public class Actor extends HollowObject {

    public Actor(ActorDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int getActorId() {
        return delegate().getActorId(ordinal);
    }



    public String getActorName() {
        return delegate().getActorName(ordinal);
    }

    public boolean isActorNameEqual(String testValue) {
        return delegate().isActorNameEqual(ordinal, testValue);
    }

    public HString getActorNameHollowReference() {
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

    public String toString() {
        return new HollowRecordStringifier().stringify(this);
    }

    /**
     * Creates a unique key index for {@code Actor} that has a primary key.
     * The primary key is represented by the type {@code int}.
     * <p>
     * By default the unique key index will not track updates to the {@code consumer} and thus
     * any changes will not be reflected in matched results.  To track updates the index must be
     * {@link HollowConsumer#addRefreshListener(HollowConsumer.RefreshListener) registered}
     * with the {@code consumer}
     *
     * @param consumer the consumer
     * @return the unique key index
     */
    public static UniqueKeyIndex<Actor, Integer> uniqueIndex(HollowConsumer consumer) {
        return UniqueKeyIndex.from(consumer, Actor.class)
            .bindToPrimaryKey()
            .usingPath("actorId", int.class);
    }

}
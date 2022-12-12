package how.hollow.consumer.api.generated.index;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.Actor;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.AbstractHollowUniqueKeyIndex;
import com.netflix.hollow.api.consumer.index.HollowUniqueKeyIndex;
import com.netflix.hollow.core.schema.HollowObjectSchema;

/**
 * @deprecated see {@link com.netflix.hollow.api.consumer.index.UniqueKeyIndex} which can be created as follows:
 * <pre>{@code
 *     UniqueKeyIndex<Actor, int> uki = Actor.uniqueIndex(consumer);
 *     int k = ...;
 *     Actor m = uki.findMatch(k);
 * }</pre>
 * @see Actor#uniqueIndex
 */
@Deprecated
@SuppressWarnings("all")
public class ActorPrimaryKeyIndex extends AbstractHollowUniqueKeyIndex<MovieAPI, Actor> {

    public ActorPrimaryKeyIndex(HollowConsumer consumer) {
        this(consumer, false);
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh) {
        this(consumer, isListenToDataRefresh, ((HollowObjectSchema)consumer.getStateEngine().getNonNullSchema("Actor")).getPrimaryKey().getFieldPaths());
    }

    private ActorPrimaryKeyIndex(HollowConsumer consumer, String... fieldPaths) {
        this(consumer, false, fieldPaths);
    }

    private ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh, String... fieldPaths) {
        super(consumer, "Actor", isListenToDataRefresh, fieldPaths);
    }

    public Actor findMatch(int actorId) {
        int ordinal = idx.getMatchingOrdinal(actorId);
        if(ordinal == -1)
            return null;
        return api.getActor(ordinal);
    }

}
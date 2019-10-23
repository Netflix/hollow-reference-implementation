package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.AbstractHollowUniqueKeyIndex;
import com.netflix.hollow.api.consumer.index.HollowUniqueKeyIndex;
import com.netflix.hollow.core.schema.HollowObjectSchema;

/**
 * @deprecated see {@link com.netflix.hollow.api.consumer.index.UniqueKeyIndex} which can be built as follows:
 * <pre>{@code
 *     UniqueKeyIndex<Actor, K> uki = UniqueKeyIndex.from(consumer, Actor.class)
 *         .usingBean(k);
 *     Actor m = uki.findMatch(k);
 * }</pre>
 * where {@code K} is a class declaring key field paths members, annotated with
 * {@link com.netflix.hollow.api.consumer.index.FieldPath}, and {@code k} is an instance of
 * {@code K} that is the key to find the unique {@code Actor} object.
 */
@Deprecated
@SuppressWarnings("all")
public class ActorPrimaryKeyIndex extends AbstractHollowUniqueKeyIndex<MovieAPI, Actor> implements HollowUniqueKeyIndex<Actor> {

    public ActorPrimaryKeyIndex(HollowConsumer consumer) {
        this(consumer, true);
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh) {
        this(consumer, isListenToDataRefresh, ((HollowObjectSchema)consumer.getStateEngine().getNonNullSchema("Actor")).getPrimaryKey().getFieldPaths());
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, String... fieldPaths) {
        this(consumer, true, fieldPaths);
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh, String... fieldPaths) {
        super(consumer, "Actor", isListenToDataRefresh, fieldPaths);
    }

    @Override
    public Actor findMatch(Object... keys) {
        int ordinal = idx.getMatchingOrdinal(keys);
        if(ordinal == -1)
            return null;
        return api.getActor(ordinal);
    }

}
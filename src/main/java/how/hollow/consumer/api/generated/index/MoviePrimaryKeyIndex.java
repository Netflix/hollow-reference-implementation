package how.hollow.consumer.api.generated.index;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.Movie;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.AbstractHollowUniqueKeyIndex;
import com.netflix.hollow.api.consumer.index.HollowUniqueKeyIndex;
import com.netflix.hollow.core.schema.HollowObjectSchema;

/**
 * @deprecated see {@link com.netflix.hollow.api.consumer.index.UniqueKeyIndex} which can be created as follows:
 * <pre>{@code
 *     UniqueKeyIndex<Movie, int> uki = Movie.uniqueIndex(consumer);
 *     int k = ...;
 *     Movie m = uki.findMatch(k);
 * }</pre>
 * @see Movie#uniqueIndex
 */
@Deprecated
@SuppressWarnings("all")
public class MoviePrimaryKeyIndex extends AbstractHollowUniqueKeyIndex<MovieAPI, Movie> {

    public MoviePrimaryKeyIndex(HollowConsumer consumer) {
        this(consumer, false);
    }

    public MoviePrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh) {
        this(consumer, isListenToDataRefresh, ((HollowObjectSchema)consumer.getStateEngine().getNonNullSchema("Movie")).getPrimaryKey().getFieldPaths());
    }

    private MoviePrimaryKeyIndex(HollowConsumer consumer, String... fieldPaths) {
        this(consumer, false, fieldPaths);
    }

    private MoviePrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh, String... fieldPaths) {
        super(consumer, "Movie", isListenToDataRefresh, fieldPaths);
    }

    public Movie findMatch(int id) {
        int ordinal = idx.getMatchingOrdinal(id);
        if(ordinal == -1)
            return null;
        return api.getMovie(ordinal);
    }

}
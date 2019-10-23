package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.UniqueKeyIndex;
import com.netflix.hollow.api.objects.HollowObject;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class Movie extends HollowObject {

    public Movie(MovieDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int getId() {
        return delegate().getId(ordinal);
    }

    public Integer getIdBoxed() {
        return delegate().getIdBoxed(ordinal);
    }

    public HString getTitle() {
        int refOrdinal = delegate().getTitleOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getHString(refOrdinal);
    }

    public SetOfActor getActors() {
        int refOrdinal = delegate().getActorsOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getSetOfActor(refOrdinal);
    }

    public MovieAPI api() {
        return typeApi().getAPI();
    }

    public MovieTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected MovieDelegate delegate() {
        return (MovieDelegate)delegate;
    }

    /**
     * Creates a unique key index for {@code Movie} that has a primary key.
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
    public static UniqueKeyIndex<Movie, Integer> uniqueIndex(HollowConsumer consumer) {
        return UniqueKeyIndex.from(consumer, Movie.class)
            .bindToPrimaryKey()
            .usingPath("id", int.class);
    }

}
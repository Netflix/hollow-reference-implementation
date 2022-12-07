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
public class Movie extends HollowObject {

    public Movie(MovieDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public int getId() {
        return delegate().getId(ordinal);
    }



    public String getTitle() {
        return delegate().getTitle(ordinal);
    }

    public boolean isTitleEqual(String testValue) {
        return delegate().isTitleEqual(ordinal, testValue);
    }

    public HString getTitleHollowReference() {
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

    public String toString() {
        return new HollowRecordStringifier().stringify(this);
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
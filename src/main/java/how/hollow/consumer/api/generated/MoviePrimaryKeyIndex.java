package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;

public class MoviePrimaryKeyIndex implements HollowConsumer.RefreshListener {

    private HollowPrimaryKeyIndex idx;
    private MovieAPI api;

    public MoviePrimaryKeyIndex(HollowConsumer consumer) {
        this(consumer, ((HollowObjectSchema)consumer.getStateEngine().getSchema("Movie")).getPrimaryKey().getFieldPaths());
    }

    public MoviePrimaryKeyIndex(HollowConsumer consumer, String... fieldPaths) {
        consumer.getRefreshLock().lock();
        try {
            this.api = (MovieAPI)consumer.getAPI();
            this.idx = new HollowPrimaryKeyIndex(consumer.getStateEngine(), "Movie", fieldPaths);
            idx.listenForDeltaUpdates();
            consumer.addRefreshListener(this);
        } catch(ClassCastException cce) {
            throw new ClassCastException("The HollowConsumer provided was not created with the MovieAPI generated API class.");
        } finally {
            consumer.getRefreshLock().unlock();
        }
    }

    public Movie findMatch(Object... keys) {
        int ordinal = idx.getMatchingOrdinal(keys);
        if(ordinal == -1)
            return null;
        return api.getMovie(ordinal);
    }

    @Override public void snapshotUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception {
        idx.detachFromDeltaUpdates();
        idx = new HollowPrimaryKeyIndex(stateEngine, idx.getPrimaryKey());
        idx.listenForDeltaUpdates();
        this.api = (MovieAPI)api;
    }

    @Override public void deltaUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception {
        this.api = (MovieAPI)api;
    }

    @Override public void refreshStarted(long currentVersion, long requestedVersion) { }
    @Override public void blobLoaded(HollowConsumer.Blob transition) { }
    @Override public void refreshSuccessful(long beforeVersion, long afterVersion, long requestedVersion) { }
    @Override public void refreshFailed(long beforeVersion, long afterVersion, long requestedVersion, Throwable failureCause) { }
}
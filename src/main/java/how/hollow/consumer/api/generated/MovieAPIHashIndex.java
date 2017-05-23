package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.core.index.HollowHashIndex;
import com.netflix.hollow.core.index.HollowHashIndexResult;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.iterator.HollowOrdinalIterator;
import java.util.Collections;
import java.lang.Iterable;
import java.util.Iterator;

public class MovieAPIHashIndex implements HollowConsumer.RefreshListener {

    private HollowHashIndex idx;
    private MovieAPI api;
    private final String queryType;    private final String selectFieldPath;
    private final String matchFieldPaths[];

    public MovieAPIHashIndex(HollowConsumer consumer, String queryType, String selectFieldPath, String... matchFieldPaths) {
        this.queryType = queryType;        this.selectFieldPath = selectFieldPath;
        this.matchFieldPaths = matchFieldPaths;
        consumer.getRefreshLock().lock();
        try {
            this.api = (MovieAPI)consumer.getAPI();
            this.idx = new HollowHashIndex(consumer.getStateEngine(), queryType, selectFieldPath, matchFieldPaths);
            consumer.addRefreshListener(this);
        } catch(ClassCastException cce) {
            throw new ClassCastException("The HollowConsumer provided was not created with the MovieAPI generated API class.");
        } finally {
            consumer.getRefreshLock().unlock();
        }
    }

    public Iterable<HString> findStringMatches(Object... keys) {
        HollowHashIndexResult matches = idx.findMatches(keys);
        if(matches == null)
            return Collections.emptySet();

        final HollowOrdinalIterator iter = matches.iterator();

        return new Iterable<HString>() {
            public Iterator<HString> iterator() {
                return new Iterator<HString>() {

                    private int next = iter.next();

                    public boolean hasNext() {
                        return next != HollowOrdinalIterator.NO_MORE_ORDINALS;
                    }

                    public HString next() {
                        HString obj = api.getHString(next);
                        next = iter.next();
                        return obj;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Actor> findActorMatches(Object... keys) {
        HollowHashIndexResult matches = idx.findMatches(keys);
        if(matches == null)
            return Collections.emptySet();

        final HollowOrdinalIterator iter = matches.iterator();

        return new Iterable<Actor>() {
            public Iterator<Actor> iterator() {
                return new Iterator<Actor>() {

                    private int next = iter.next();

                    public boolean hasNext() {
                        return next != HollowOrdinalIterator.NO_MORE_ORDINALS;
                    }

                    public Actor next() {
                        Actor obj = api.getActor(next);
                        next = iter.next();
                        return obj;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<SetOfActor> findSetOfActorMatches(Object... keys) {
        HollowHashIndexResult matches = idx.findMatches(keys);
        if(matches == null)
            return Collections.emptySet();

        final HollowOrdinalIterator iter = matches.iterator();

        return new Iterable<SetOfActor>() {
            public Iterator<SetOfActor> iterator() {
                return new Iterator<SetOfActor>() {

                    private int next = iter.next();

                    public boolean hasNext() {
                        return next != HollowOrdinalIterator.NO_MORE_ORDINALS;
                    }

                    public SetOfActor next() {
                        SetOfActor obj = api.getSetOfActor(next);
                        next = iter.next();
                        return obj;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Movie> findMovieMatches(Object... keys) {
        HollowHashIndexResult matches = idx.findMatches(keys);
        if(matches == null)
            return Collections.emptySet();

        final HollowOrdinalIterator iter = matches.iterator();

        return new Iterable<Movie>() {
            public Iterator<Movie> iterator() {
                return new Iterator<Movie>() {

                    private int next = iter.next();

                    public boolean hasNext() {
                        return next != HollowOrdinalIterator.NO_MORE_ORDINALS;
                    }

                    public Movie next() {
                        Movie obj = api.getMovie(next);
                        next = iter.next();
                        return obj;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override public void deltaUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception {
        reindex(stateEngine, api);
    }

    @Override public void snapshotUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception {
        reindex(stateEngine, api);
    }

    private void reindex(HollowReadStateEngine stateEngine, HollowAPI api) {
        this.idx = new HollowHashIndex(stateEngine, queryType, selectFieldPath, matchFieldPaths);
        this.api = (MovieAPI) api;
    }

    @Override public void refreshStarted(long currentVersion, long requestedVersion) { }
    @Override public void blobLoaded(HollowConsumer.Blob transition) { }
    @Override public void refreshSuccessful(long beforeVersion, long afterVersion, long requestedVersion) { }
    @Override public void refreshFailed(long beforeVersion, long afterVersion, long requestedVersion, Throwable failureCause) { }

}
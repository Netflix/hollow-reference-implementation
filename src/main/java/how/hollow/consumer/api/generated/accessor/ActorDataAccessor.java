package how.hollow.consumer.api.generated.accessor;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.Actor;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.data.AbstractHollowDataAccessor;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;

@SuppressWarnings("all")
public class ActorDataAccessor extends AbstractHollowDataAccessor<Actor> {

    public static final String TYPE = "Actor";
    private MovieAPI api;

    public ActorDataAccessor(HollowConsumer consumer) {
        super(consumer, TYPE);
        this.api = (MovieAPI)consumer.getAPI();
    }

    public ActorDataAccessor(HollowReadStateEngine rStateEngine, MovieAPI api) {
        super(rStateEngine, TYPE);
        this.api = api;
    }

    public ActorDataAccessor(HollowReadStateEngine rStateEngine, MovieAPI api, String ... fieldPaths) {
        super(rStateEngine, TYPE, fieldPaths);
        this.api = api;
    }

    public ActorDataAccessor(HollowReadStateEngine rStateEngine, MovieAPI api, PrimaryKey primaryKey) {
        super(rStateEngine, TYPE, primaryKey);
        this.api = api;
    }

    @Override public Actor getRecord(int ordinal){
        return api.getActor(ordinal);
    }

}
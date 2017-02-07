package how.hollow.consumer.api.generated;

import java.util.Collections;
import java.util.Set;

import com.netflix.hollow.api.client.HollowAPIFactory;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.api.objects.provider.HollowFactory;
import com.netflix.hollow.core.read.dataaccess.HollowDataAccess;

public class MovieAPIFactory implements HollowAPIFactory {

    private final Set<String> cachedTypes;

    public MovieAPIFactory() {
        this(Collections.<String>emptySet());
    }

    public MovieAPIFactory(Set<String> cachedTypes) {
        this.cachedTypes = cachedTypes;
    }

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess) {
        return new MovieAPI(dataAccess);
    }

    @Override
    public HollowAPI createAPI(HollowDataAccess dataAccess, HollowAPI previousCycleAPI) {
        return new MovieAPI(dataAccess, cachedTypes, Collections.<String, HollowFactory<?>>emptyMap(), (MovieAPI) previousCycleAPI);
    }

}
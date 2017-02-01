package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.delegate.HollowObjectAbstractDelegate;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.api.custom.HollowTypeAPI;
import com.netflix.hollow.api.objects.delegate.HollowCachedDelegate;

@SuppressWarnings("all")
public class MovieDelegateCachedImpl extends HollowObjectAbstractDelegate implements HollowCachedDelegate, MovieDelegate {

    private final Integer id;
    private final int titleOrdinal;
    private final int actorsOrdinal;
   private MovieTypeAPI typeAPI;

    public MovieDelegateCachedImpl(MovieTypeAPI typeAPI, int ordinal) {
        this.id = typeAPI.getIdBoxed(ordinal);
        this.titleOrdinal = typeAPI.getTitleOrdinal(ordinal);
        this.actorsOrdinal = typeAPI.getActorsOrdinal(ordinal);
        this.typeAPI = typeAPI;
    }

    public int getId(int ordinal) {
        return id.intValue();
    }

    public Integer getIdBoxed(int ordinal) {
        return id;
    }

    public int getTitleOrdinal(int ordinal) {
        return titleOrdinal;
    }

    public int getActorsOrdinal(int ordinal) {
        return actorsOrdinal;
    }

    @Override
    public HollowObjectSchema getSchema() {
        return typeAPI.getTypeDataAccess().getSchema();
    }

    @Override
    public HollowObjectTypeDataAccess getTypeDataAccess() {
        return typeAPI.getTypeDataAccess();
    }

    public MovieTypeAPI getTypeAPI() {
        return typeAPI;
    }

    public void updateTypeAPI(HollowTypeAPI typeAPI) {
        this.typeAPI = (MovieTypeAPI) typeAPI;
    }

}
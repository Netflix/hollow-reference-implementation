package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface MovieDelegate extends HollowObjectDelegate {

    public int getId(int ordinal);

    public Integer getIdBoxed(int ordinal);

    public int getTitleOrdinal(int ordinal);

    public int getActorsOrdinal(int ordinal);

    public MovieTypeAPI getTypeAPI();

}
package com.netflix.hollow.example.consumer.api.generated;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface ActorDelegate extends HollowObjectDelegate {

    public int getActorId(int ordinal);

    public Integer getActorIdBoxed(int ordinal);

    public int getActorNameOrdinal(int ordinal);

    public ActorTypeAPI getTypeAPI();

}
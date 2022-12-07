package how.hollow.consumer.api.generated.core;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface ActorDelegate extends HollowObjectDelegate {

    public int getActorId(int ordinal);

    public Integer getActorIdBoxed(int ordinal);

    public String getActorName(int ordinal);

    public boolean isActorNameEqual(int ordinal, String testValue);

    public int getActorNameOrdinal(int ordinal);

    public ActorTypeAPI getTypeAPI();

}
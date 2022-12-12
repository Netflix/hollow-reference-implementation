package how.hollow.consumer.api.generated.core;

import com.netflix.hollow.core.type.*;
import how.hollow.consumer.api.generated.core.*;
import how.hollow.consumer.api.generated.collections.*;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface MovieDelegate extends HollowObjectDelegate {

    public int getId(int ordinal);

    public Integer getIdBoxed(int ordinal);

    public String getTitle(int ordinal);

    public boolean isTitleEqual(int ordinal, String testValue);

    public int getTitleOrdinal(int ordinal);

    public int getActorsOrdinal(int ordinal);

    public MovieTypeAPI getTypeAPI();

}
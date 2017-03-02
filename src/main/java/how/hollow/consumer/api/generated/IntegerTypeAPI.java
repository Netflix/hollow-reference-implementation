package how.hollow.consumer.api.generated;

import com.netflix.hollow.api.custom.HollowObjectTypeAPI;
import com.netflix.hollow.core.read.dataaccess.HollowObjectTypeDataAccess;

@SuppressWarnings("all")
public class IntegerTypeAPI extends HollowObjectTypeAPI {

    private final IntegerDelegateLookupImpl delegateLookupImpl;

    IntegerTypeAPI(MovieAPI api, HollowObjectTypeDataAccess typeDataAccess) {
        super(api, typeDataAccess, new String[] {
            "value"
        });
        this.delegateLookupImpl = new IntegerDelegateLookupImpl(this);
    }

    public int getValue(int ordinal) {
        if(fieldIndex[0] == -1)
            return missingDataHandler().handleInt("Integer", ordinal, "value");
        return getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
    }

    public Integer getValueBoxed(int ordinal) {
        int i;
        if(fieldIndex[0] == -1) {
            i = missingDataHandler().handleInt("Integer", ordinal, "value");
        } else {
            boxedFieldAccessSampler.recordFieldAccess(fieldIndex[0]);
            i = getTypeDataAccess().readInt(ordinal, fieldIndex[0]);
        }
        if(i == Integer.MIN_VALUE)
            return null;
        return Integer.valueOf(i);
    }



    public IntegerDelegateLookupImpl getDelegateLookupImpl() {
        return delegateLookupImpl;
    }

    @Override
    public MovieAPI getAPI() {
        return (MovieAPI) api;
    }

}
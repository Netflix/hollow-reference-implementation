package how.hollow.consumer.infrastructure;

import com.netflix.hollow.api.consumer.HollowConsumer;

import java.io.IOException;
import java.io.InputStream;

public class GCSBlob  extends HollowConsumer.Blob {
    private StorageRequester storageRequester;

    GCSBlob(StorageRequester storageRequester, long toVersion) {
        super(toVersion);
        this.storageRequester = storageRequester;
    }

    GCSBlob(StorageRequester storageRequester, long fromVersion, long toVersion) {
        super(fromVersion, toVersion);
        this.storageRequester = storageRequester;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return storageRequester.getInputStream();
    }
}

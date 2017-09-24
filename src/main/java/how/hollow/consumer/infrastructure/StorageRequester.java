package how.hollow.consumer.infrastructure;

import com.google.api.client.http.HttpResponseException;
import com.google.api.services.storage.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class StorageRequester {
    private final Storage.Objects.Get rawRequest;

    StorageRequester(Storage.Objects.Get rawRequest) {
        this.rawRequest = rawRequest;
    }

    Map<String, String> getMetadata() throws IOException {
        try {
            return rawRequest.execute().getMetadata();
        } catch (HttpResponseException e) {
            return throwNotFoundOrException(e);
        }
    }

    InputStream getInputStream() throws IOException {
        try {
            return rawRequest.executeMediaAsInputStream();
        } catch (HttpResponseException e) {
            return throwNotFoundOrException(e);
        }
    }

    private static <T> T throwNotFoundOrException(HttpResponseException e) throws HttpResponseException {
        if (e.getStatusCode() == 404) {
            throw new BlobNotFoundException();
        }

        throw e;
    }
}

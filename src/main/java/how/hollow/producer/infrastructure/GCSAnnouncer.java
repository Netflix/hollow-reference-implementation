package how.hollow.producer.infrastructure;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.netflix.hollow.api.producer.HollowProducer;
import how.hollow.utils.GCSUtils;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

public class GCSAnnouncer implements HollowProducer.Announcer {

    private static Logger log = Logger.getLogger("GCSAnnouncer");

    private static final String UTF_8 = "UTF-8";
    private static final String TEXT_PLAIN = "text/plain";

    private String bucket;
    private Storage storage;
    private String logPrefix;
    private String folder;
    private String versionBlobName;

    GCSAnnouncer(String domain, String bucketName, String namespace, String applicationName) {
        bucket = bucketName;
        folder = GCSUtils.getFolder(domain, namespace);
        versionBlobName = folder + "/version";
        logPrefix = "(" + folder + ") ";
        storage = GCSUtils.getStorage(applicationName);
    }

    @Override
    public void announce(long stateVersion) {
        try {
            ByteArrayContent content = new ByteArrayContent(TEXT_PLAIN, String.valueOf(stateVersion).getBytes(UTF_8));
            uploadObject(versionBlobName, null, content);
        } catch (Exception e) {
            log.warning(logPrefix + "error uploading version: " + stateVersion);
        }
        log.info(logPrefix + "uploaded version object: " + stateVersion);
    }

    private void uploadObject(String blobName, Map<String, String> metadata, AbstractInputStreamContent content) throws IOException {
        log.info(logPrefix + "uploading object: " + blobName);
        StorageObject metadataObject = new StorageObject().setMetadata(metadata);
        Storage.Objects.Insert insertObject = storage.objects().insert(bucket, metadataObject, content).setName(blobName);
        insertObject.getMediaHttpUploader().setDisableGZipContent(true);
        insertObject.execute();
    }
}

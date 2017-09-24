package how.hollow.consumer.infrastructure;

import com.google.api.services.storage.Storage;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.producer.HollowProducer;
import how.hollow.utils.GCSUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.logging.Logger;

public class GCSBlobRetriever implements HollowConsumer.BlobRetriever {

    private static Logger log = Logger.getLogger("GCSBlobRetriever");

    public static final String TO_STATE = "to_state";
    public static final String FROM_STATE = "from_state";

    private String bucket;
    private String folder;
    private String logPrefix;
    private Storage storage;

    GCSBlobRetriever(String domain, String bucketName, String namespace, String applicationName) {
        bucket = bucketName;
        folder = GCSUtils.getFolder(domain, namespace);
        logPrefix = "(" + folder + ") ";
        storage = GCSUtils.getStorage(applicationName);
    }

    @Override
    public HollowConsumer.Blob retrieveSnapshotBlob(long desiredVersion) {
        HollowConsumer.Blob hollowBlob = getSnapshotBlob(desiredVersion);
        if (hollowBlob != null) {
            log.info(logPrefix + "downloading latest snapshot: " + desiredVersion);
            return hollowBlob;
        }

        String snapshotIndexBlobName = GCSUtils.getSnapshotIndexObjectName(folder);
        try {
            InputStream inputStream = getStorageRequester(snapshotIndexBlobName).getInputStream();
            long version = GCSUtils.findNextHighestVersion(desiredVersion, inputStream);
            if (version == Long.MIN_VALUE) {
                return null;
            }

            log.info(logPrefix + "downloading latest snapshot: " + version + " (requested:" + desiredVersion + ")");
            return getSnapshotBlob(version);
        } catch (Exception e) {
            throw new RuntimeException("error downloading snapshot index (bucket:" + bucket + ", blob:" + snapshotIndexBlobName + ")", e);
        }

    }

    @Override
    public HollowConsumer.Blob retrieveDeltaBlob(long currentVersion) {
        try {
            log.info(logPrefix + "downloading delta from version: " + currentVersion);
            return getDeltaBlob(HollowProducer.Blob.Type.DELTA.prefix, currentVersion);
        } catch (RuntimeException e) {
            throw new RuntimeException("Error retrieving delta blob (bucket:" + bucket + ", version:" + currentVersion + ")");
        }
    }

    @Override
    public HollowConsumer.Blob retrieveReverseDeltaBlob(long currentVersion) {
        try {
            log.info(logPrefix + "downloading reversedelta from version: " + currentVersion);
            return getDeltaBlob(HollowProducer.Blob.Type.REVERSE_DELTA.prefix, currentVersion);
        } catch (RuntimeException e) {
            throw new RuntimeException("Error retrieving reversedelta blob (bucket:" + bucket + ", version:" + currentVersion + ")");
        }
    }


    protected HollowConsumer.Blob getSnapshotBlob(long desiredVersion) {
        try {
            StorageRequester storageRequester = getStorageRequester(GCSUtils.getBlobName(folder, HollowProducer.Blob.Type.SNAPSHOT.prefix, desiredVersion));
            storageRequester.getMetadata(); // this will throw a NotFoundException if the object doesn't exist
            return new GCSBlob(storageRequester, desiredVersion);
        } catch (BlobNotFoundException e) {
            log.info(logPrefix + "snapshot object not found: " + desiredVersion);
            return null;
        } catch (IOException e) {
            throw new RuntimeException("error downloading snapshot object: " + desiredVersion, e);
        }
    }

    protected HollowConsumer.Blob getDeltaBlob(String fileType, long fromVersion) {
        String blobName = GCSUtils.getBlobName(folder, fileType, fromVersion);

        try {
            StorageRequester storageRequester = getStorageRequester(blobName);
            Map<String, String> metadata = storageRequester.getMetadata();
            Long fromState = Long.parseLong(metadata.get(FROM_STATE));
            Long toState = Long.parseLong(metadata.get(TO_STATE));
            return new GCSBlob(storageRequester, fromState, toState);
        } catch (BlobNotFoundException e) {
            log.info(logPrefix + "delta object not found: " + fromVersion);
            return null;
        } catch (IOException e) {
            throw new RuntimeException("error downloading delta object: " + fromVersion, e);
        }
    }

    protected StorageRequester getStorageRequester(String blobName) throws IOException {
        return new StorageRequester(storage.objects().get(bucket, blobName));
    }
}
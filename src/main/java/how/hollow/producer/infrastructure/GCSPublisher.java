package how.hollow.producer.infrastructure;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.FileContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.core.memory.encoding.VarInt;
import how.hollow.consumer.infrastructure.BlobNotFoundException;
import how.hollow.utils.GCSUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class GCSPublisher implements HollowProducer.Publisher {

    private static Logger log = Logger.getLogger("GCSPublisher");

    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    private static final String TO_STATE = "to_state";

    private static final String FROM_STATE = "from_state";

    private String bucket;
    private String folder;
    private String logPrefix;
    private Storage storage;
    private List<Long> snapshotIndex;

    GCSPublisher(String domain, String bucketName, String namespace, String applicationName) {
        bucket = bucketName;
        folder = GCSUtils.getFolder(domain, namespace);
        logPrefix = "(" + folder + ") ";
        storage = GCSUtils.getStorage(applicationName);
        snapshotIndex = initializeSnapshotIndex();
    }

    @Override
    public void publish(HollowProducer.Blob blob) {
        switch (blob.getType()) {
            case SNAPSHOT:
                publishSnapshot(blob);
                break;
            case DELTA:
                publishDelta(blob);
                break;
            case REVERSE_DELTA:
                publishReverseDelta(blob);
                break;
        }
    }

    public void publishSnapshot(HollowProducer.Blob blob) {
        long toVersion = blob.getToVersion();
        String blobName = GCSUtils.getBlobName(folder, HollowProducer.Blob.Type.SNAPSHOT.prefix, toVersion);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(TO_STATE, String.valueOf(toVersion));
        uploadFile(blob.getFile(), blobName, metadata);
        log.info(logPrefix + "uploaded snapshot: " + toVersion);
        // now we update the snapshot index
        updateSnapshotIndex(toVersion);
        log.info(logPrefix + "uploaded snapshot index: " + toVersion);
    }

    public void publishDelta(HollowProducer.Blob blob) {
        long fromVersion = blob.getFromVersion();
        long toVersion = blob.getToVersion();
        String blobName = GCSUtils.getBlobName(folder, HollowProducer.Blob.Type.DELTA.prefix, fromVersion);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(FROM_STATE, String.valueOf(fromVersion));
        metadata.put(TO_STATE, String.valueOf(toVersion));
        uploadFile(blob.getFile(), blobName, metadata);
        log.info(logPrefix + "uploaded delta: " + fromVersion + " -> " + toVersion);
    }

    public void publishReverseDelta(HollowProducer.Blob blob) {
        long fromVersion = blob.getFromVersion();
        long toVersion = blob.getToVersion();
        String blobName = GCSUtils.getBlobName(folder, HollowProducer.Blob.Type.REVERSE_DELTA.prefix, fromVersion);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(FROM_STATE, String.valueOf(fromVersion));
        metadata.put(TO_STATE, String.valueOf(toVersion));
        uploadFile(blob.getFile(), blobName, metadata);
        log.info(logPrefix + "uploaded delta: " + fromVersion + " -> " + toVersion);
    }

    /**
     * Write a list of all of the state versions to GCS.
     *
     * @param newVersion
     */
    private synchronized void updateSnapshotIndex(Long newVersion) {
        /// insert the new version into the list
        int idx = Collections.binarySearch(snapshotIndex, newVersion);
        int insertionPoint = Math.abs(idx) - 1;
        snapshotIndex.add(insertionPoint, newVersion);

        /// build a binary representation of the list -- gap encoded variable-length integers
        byte[] idxBytes = buidGapEncodedVarIntSnapshotIndex();

        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("Content-Length", String.valueOf((long) idxBytes.length));

        try {
            AbstractInputStreamContent content = new ByteArrayContent(APPLICATION_OCTET_STREAM, idxBytes);
            uploadObject(GCSUtils.getSnapshotIndexObjectName(folder), null, content);
        } catch (Exception e) {
            throw new RuntimeException("error uploading snapshot index object (version:" + newVersion + ")", e);
        }

    }

    /**
     * Encode the sorted list of all state versions as gap-encoded variable length integers.
     *
     * @return
     */
    private byte[] buidGapEncodedVarIntSnapshotIndex() {
        int idx;
        byte[] idxBytes;
        idx = 0;
        long currentSnapshotId = snapshotIndex.get(idx++);
        long currentSnapshotIdGap = currentSnapshotId;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            while (idx < snapshotIndex.size()) {
                VarInt.writeVLong(os, currentSnapshotIdGap);

                long nextSnapshotId = snapshotIndex.get(idx++);
                currentSnapshotIdGap = nextSnapshotId - currentSnapshotId;
                currentSnapshotId = nextSnapshotId;
            }

            VarInt.writeVLong(os, currentSnapshotIdGap);

            idxBytes = os.toByteArray();
        } catch (IOException shouldNotHappen) {
            throw new RuntimeException(shouldNotHappen);
        }

        return idxBytes;
    }

    /**
     * Find all of the existing snapshots.
     */
    private List<Long> initializeSnapshotIndex() {
        List<Long> versions = new ArrayList<>();

        try {
            Storage.Objects.List listObjects = storage.objects().list(bucket).setPrefix(GCSUtils.getGCSObjectPrefix(folder, HollowProducer.Blob.Type.SNAPSHOT.prefix));
            Objects objects = listObjects.execute();

            while (objects.getNextPageToken() != null) {
                List<StorageObject> items = objects.getItems();

                for(StorageObject item : items) {
                    addSnapshotStateId(item, versions);
                }
                listObjects.setPageToken(objects.getNextPageToken());
            }
        } catch (BlobNotFoundException e) {
            log.info(logPrefix + "snapshot index object not found in storage");
        } catch (IOException e) {
            throw new RuntimeException("error initializing snapshot index", e);
        }

        return versions;
    }

    protected static void addSnapshotStateId(StorageObject storageObject, List<Long> snapshotIndex) {
        String name = storageObject.getName();
        try {
            snapshotIndex.add(Long.parseLong(name.substring(name.lastIndexOf("-") + 1)));
        } catch (NumberFormatException e) {
            throw new RuntimeException("error adding snapshot state id (name:" + name + ")", e);
        }
    }

    protected StorageObject uploadFile(File scratchFile, String blobName, Map<String, String> metadata) {
        try {
            metadata.put("Content-Length", String.valueOf(scratchFile.length()));
            FileContent content = new FileContent(APPLICATION_OCTET_STREAM, scratchFile);
            return uploadObject(blobName, metadata, content);
        } catch (IOException e) {
            throw new RuntimeException("error uploading scratch file (bucket:" + bucket + ", blob:" + blobName + ")", e);
        }
    }

    protected StorageObject uploadObject(String blobName, Map<String, String> metadata, AbstractInputStreamContent content) throws IOException {
        log.info(logPrefix + "uploading object: " + blobName);
        StorageObject metadataObject = new StorageObject().setMetadata(metadata);
        Storage.Objects.Insert insertObject = storage.objects().insert(bucket, metadataObject, content).setName(blobName);
        insertObject.getMediaHttpUploader().setDisableGZipContent(true);
        return insertObject.execute();
    }
}

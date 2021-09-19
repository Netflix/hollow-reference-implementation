package how.hollow.producer.infrastructure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.netflix.hollow.api.producer.HollowProducer.Blob;
import com.netflix.hollow.api.producer.HollowProducer.Publisher;
import com.netflix.hollow.core.memory.encoding.HashCodes;
import com.netflix.hollow.core.memory.encoding.VarInt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureBlobPublisher implements Publisher {

    private final BlobContainerClient azureClient;
    private final String containerName;
    private final String blobName;
    private final List<Long> snapshotIndex;

    public AzureBlobPublisher(String connectionString, String containerName, String blobName) {

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).
          buildClient();
        BlobContainerClient blobContainerClient = null;
        try {
            blobContainerClient = blobServiceClient.createBlobContainer(containerName);
        } catch (BlobStorageException exception) {
            if (exception.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
                //System.out.println("Container already exists");
                // Create the container and return a container client object
                blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
          }
        }
        this.azureClient = blobContainerClient;
        this.containerName = containerName;
        this.blobName = blobName;
        this.snapshotIndex = initializeSnapshotIndex();
    }

    @Override
    public void publish(Blob blob) {
        switch(blob.getType()) {
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

    public void publishSnapshot(Blob blob) {
        String objectName = getBlobName(blobName, "snapshot", blob.getToVersion());
        Map<String, String> map = new HashMap<>();
        map.put("to_state", String.valueOf(blob.getToVersion()));
        uploadBlob(blob, objectName, map);

        // now we update the snapshot index
        updateSnapshotIndex(blob.getToVersion());
    }

    public void publishDelta(Blob blob) {
        String objectName = getBlobName(blobName, "delta", blob.getFromVersion());

        Map<String, String> map = new HashMap<>();
        map.put("from_state", String.valueOf(blob.getFromVersion()));
        map.put("to_state", String.valueOf(blob.getToVersion()));
        uploadBlob(blob, objectName, map);
    }

    public void publishReverseDelta(Blob blob) {
        String objectName = getBlobName(blobName, "reversedelta", blob.getFromVersion());

        Map<String, String> map = new HashMap<>();
        map.put("from_state", String.valueOf(blob.getFromVersion()));
        map.put("to_state", String.valueOf(blob.getToVersion()));
        uploadBlob(blob, objectName, map);
    }

    public static String getAzureObjectName(String blobNamespace, String fileType, long lookupVersion) {
        StringBuilder builder = new StringBuilder(getAzureObjectPrefix(blobNamespace, fileType));
        builder.append(Integer.toHexString(HashCodes.hashLong(lookupVersion)));
        builder.append("-");
        builder.append(lookupVersion);
        return builder.toString();
    }

    public static String getBlobName(String blobNamespace, String fileType, long lookupVersion) {
        StringBuilder builder = new StringBuilder(getAzureObjectPrefix(blobNamespace, fileType));
        builder.append(Integer.toHexString(HashCodes.hashLong(lookupVersion)));
        builder.append("-");
        builder.append(lookupVersion);
        return builder.toString();
    }

    private static String getAzureObjectPrefix(String blobNamespace, String fileType) {
        StringBuilder builder = new StringBuilder(blobNamespace);
        builder.append("/").append(fileType).append("/");
        return builder.toString();
    }

    private void uploadBlob(Blob blob, String blobName, Map<String, String> metadata) {
        try (InputStream inputStream = blob.newInputStream()) {
            azureClient.getBlobClient(blobName).uploadWithResponse(inputStream, inputStream.available(),
            null, null, metadata, null, null, null, null);
        } catch (Exception exception) {
            throw new RuntimeException("error uploading blob to azure blob storage", exception);
        }
    }

    /////////////////////// BEGIN SNAPSHOT INDEX CODE ///////////////////////

	  public static String getSnapshotIndexObjectName(String blobNamespace) {
		return blobNamespace + "/snapshot.index";
	}

    /**
     * Write a list of all of the state versions to Azure Blob Storage.
     * @param newVersion
     */
    private synchronized void updateSnapshotIndex(Long newVersion) {
    	/// insert the new version into the list
        int idx = Collections.binarySearch(snapshotIndex, newVersion);
        int insertionPoint = Math.abs(idx) - 1;
        snapshotIndex.add(insertionPoint, newVersion);

        // build a binary representation of the list -- gap encoded variable-length integers
        byte[] idxBytes = buidGapEncodedVarIntSnapshotIndex();

           // upload the new file content.
            try(InputStream is = new ByteArrayInputStream(idxBytes)) {
                azureClient.getBlobClient(getSnapshotIndexObjectName(blobName)).uploadWithResponse(is, idxBytes.length,
                null, null, null, null, null, null,
                null);

            } catch(Exception e) {
                throw new RuntimeException(e);
            }
    }

    /**
     * Encode the sorted list of all state versions as gap-encoded variable length integers.
     * @return
     */
    private byte[] buidGapEncodedVarIntSnapshotIndex() {
        int idx;
        byte[] idxBytes;
        idx = 0;
        long currentSnapshotId = snapshotIndex.get(idx++);
        long currentSnapshotIdGap = currentSnapshotId;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            while(idx < snapshotIndex.size()) {
              VarInt.writeVLong(os, currentSnapshotIdGap);

              long nextSnapshotId = snapshotIndex.get(idx++);
              currentSnapshotIdGap = nextSnapshotId - currentSnapshotId;
              currentSnapshotId = nextSnapshotId;
            }

            VarInt.writeVLong(os, currentSnapshotIdGap);

            idxBytes = os.toByteArray();
        } catch(IOException shouldNotHappen) {
            throw new RuntimeException(shouldNotHappen);
        }

        return idxBytes;
    }

    /**
     * Find all of the existing snapshots.
     */
    private List<Long> initializeSnapshotIndex() {
        List<Long> snapshotIdx = new ArrayList<>();

        ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(getAzureObjectPrefix(blobName, "snapshot"));
        PagedIterable<BlobItem> listObjects = azureClient.listBlobs(options, null);

        listObjects.streamByPage().forEach(pagedResponse -> pagedResponse.getElements()
          .forEach(blobItem -> addSnapshotStateId(blobItem, snapshotIdx)));

        Collections.sort(snapshotIdx);
        return snapshotIdx;
    }

    private void addSnapshotStateId(BlobItem obj, List<Long> snapshotIdx) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneId.of("Z"));
            String key = obj.getProperties().getLastModified().format(dateTimeFormatter);
            try {
              snapshotIdx.add(Long.parseLong(key));
            } catch(NumberFormatException ignore) { }
    }

}

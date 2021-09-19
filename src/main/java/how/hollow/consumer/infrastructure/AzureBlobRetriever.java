package how.hollow.consumer.infrastructure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.netflix.hollow.api.consumer.HollowConsumer.Blob;
import com.netflix.hollow.api.consumer.HollowConsumer.BlobRetriever;
import com.netflix.hollow.core.memory.encoding.VarInt;
import how.hollow.producer.infrastructure.AzureBlobPublisher;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class AzureBlobRetriever implements BlobRetriever {

    private final BlobContainerClient azureClient;
    private final String containerName;
    private final String blobName;

    public AzureBlobRetriever(String connectionString, String containerName, String blobName) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).
          buildClient();
        this.azureClient = blobServiceClient.getBlobContainerClient(containerName);
        this.containerName = containerName;
        this.blobName = blobName;
    }

    @Override
    public Blob retrieveSnapshotBlob(long desiredVersion) {
        try {
            return knownSnapshotBlob(desiredVersion);
        } catch (BlobStorageException transitionNotFound) { }

        /// There was no exact match for a snapshot leading to the desired state.
        /// We'll use the snapshot index to find the nearest one before the desired state.
        try {
            File f = downloadFile(AzureBlobPublisher.getSnapshotIndexObjectName(blobName));
            long snapshotIdxLength = f.length();
            long pos = 0;
            long currentSnapshotStateId = 0;

            try(InputStream is = new BufferedInputStream(new FileInputStream(f))) {
                while(pos < snapshotIdxLength) {
                    long nextGap = VarInt.readVLong(is);

                    if(currentSnapshotStateId + nextGap > desiredVersion) {
                        if(currentSnapshotStateId == 0)
                            return null;

                        return knownSnapshotBlob(currentSnapshotStateId);
                    }

                    currentSnapshotStateId += nextGap;
                    pos += VarInt.sizeOfVLong(nextGap);
                }

                  if(currentSnapshotStateId != 0)
                      return knownSnapshotBlob(currentSnapshotStateId);
            }
        } catch(IOException e) {
        	  throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    public Blob retrieveDeltaBlob(long currentVersion) {
        try {
            return knownDeltaBlob("delta", currentVersion);
        } catch (BlobStorageException transitionNotFound) {
            return null;
        }
    }

    @Override
    public Blob retrieveReverseDeltaBlob(long currentVersion) {
        try {
            return knownDeltaBlob("reversedelta", currentVersion);
        } catch (BlobStorageException transitionNotFound) {
            return null;
        }
    }



    private Blob knownSnapshotBlob(long desiredVersion) {
        String objectName = AzureBlobPublisher.getAzureObjectName(blobName, "snapshot", desiredVersion);
        long toState = Long.parseLong(azureClient.getBlobClient(objectName).getProperties().getMetadata()
          .get("to_state"));

        return new AzureBlob(objectName, toState);
    }

    private Blob knownDeltaBlob(String fileType, long fromVersion) {
        String objectName = AzureBlobPublisher.getAzureObjectName(blobName, fileType, fromVersion);
        BlobProperties blobProperties  = azureClient.getBlobClient(objectName).getProperties();
        long toState = Long.parseLong(blobProperties.getMetadata().get("to_state"));
        long fromState = Long.parseLong(blobProperties.getMetadata().get("from_state"));

        return new AzureBlob(objectName, fromState, toState);
    }

    private class AzureBlob extends Blob {

        private final String objectName;

        public AzureBlob(String objectName, long toVersion) {
            super(toVersion);
            this.objectName = objectName;
        }

        public AzureBlob(String objectName, long fromVersion, long toVersion) {
            super(fromVersion, toVersion);
            this.objectName = objectName;
        }

        @Override
        public InputStream getInputStream() throws IOException {

        	final File tempFile = downloadFile(objectName);

            return new BufferedInputStream(new FileInputStream(tempFile)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    tempFile.delete();
                }
            };

        }

    }

    private File downloadFile(String objectName) {
        File tempFile = new File(System.getProperty("java.io.tmpdir"), objectName.replace('/', '-'));

        System.out.println("\nDownloading blob: " + objectName + " to\n\t " + tempFile.getAbsolutePath());
        try {
            azureClient.getBlobClient(objectName).downloadToFile(tempFile.getPath(), true);
        } catch (UncheckedIOException exception) {
            throw new RuntimeException(exception);
        }
        return tempFile;
    }

}

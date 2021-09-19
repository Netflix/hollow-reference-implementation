package how.hollow.producer.infrastructure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.netflix.hollow.api.producer.HollowProducer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class AzureAnnouncer implements HollowProducer.Announcer {

    public static final String ANNOUNCEMENT_OBJECTNAME = "announced.version";

    private final BlobContainerClient azureClient;
    private final String containerName;
    private final String blobName;

    public AzureAnnouncer(String connectionString, String containerName, String blobName) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).
          buildClient();
        BlobContainerClient blobContainerClient = null;
        try {
            blobContainerClient = blobServiceClient.createBlobContainer(containerName);
        } catch (BlobStorageException exception) {
            //System.out.println("BlobStorageException");
            if (exception.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
              //System.out.println("Container already exists");
              // Create the container and return a container client object
              blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            }
        }
        this.azureClient = blobContainerClient;
        this.containerName = containerName;
        this.blobName = blobName;
    }

    @Override
    public void announce(long stateVersion) {
        byte[] idxBytes = String.valueOf(stateVersion).getBytes();
        /// upload the new file content.
        try(InputStream is = new ByteArrayInputStream(idxBytes)) {
            azureClient.getBlobClient(blobName + "/" + ANNOUNCEMENT_OBJECTNAME).upload(is, idxBytes.length,
              true);

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


}

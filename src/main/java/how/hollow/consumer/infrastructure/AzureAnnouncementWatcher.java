package how.hollow.consumer.infrastructure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.HollowConsumer.AnnouncementWatcher;
import how.hollow.producer.infrastructure.AzureAnnouncer;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AzureAnnouncementWatcher implements AnnouncementWatcher {

    private final BlobContainerClient azureClient;
    private final String containerName;
    private final String blobName;

    private final List<HollowConsumer> subscribedConsumers;

    private long latestVersion;

    public AzureAnnouncementWatcher(String connectionString, String containerName, String blobName) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).
          buildClient();
        this.azureClient = blobServiceClient.getBlobContainerClient(containerName);
        this.containerName = containerName;
        this.blobName = blobName;
        this.subscribedConsumers = Collections.synchronizedList(new ArrayList<>());

        this.latestVersion = readLatestVersion();

        setupPollingThread();
    }

    public void setupPollingThread() {
        Thread t = new Thread(() -> {
            while(true) {
                try {
                    long currentVersion = readLatestVersion();
                    if(latestVersion != currentVersion) {
                        latestVersion = currentVersion;
                        for(HollowConsumer consumer : subscribedConsumers)
                            consumer.triggerAsyncRefresh();
                    }

                    Thread.sleep(1000);
                } catch(Throwable th) {
                    th.printStackTrace();
                }
            }
        });

        t.setName("hollow-azure-announcementwatcher-poller");
        t.setDaemon(true);
        t.start();
    }

    @Override
    public long getLatestVersion() {
        return latestVersion;
    }

    @Override
    public void subscribeToUpdates(HollowConsumer consumer) {
        subscribedConsumers.add(consumer);
    }


    private long readLatestVersion() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100);
        try {
            azureClient.getBlobClient(blobName + "/" + AzureAnnouncer.ANNOUNCEMENT_OBJECTNAME)
              .download(outputStream);
        } catch (BlobStorageException e) {
            return AnnouncementWatcher.NO_ANNOUNCEMENT_AVAILABLE;
        }
        return Long.parseLong(new String (outputStream.toByteArray()));
    }

}

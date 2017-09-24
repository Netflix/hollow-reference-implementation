package how.hollow.consumer.infrastructure;

import com.google.api.services.storage.Storage;
import com.netflix.hollow.api.consumer.HollowConsumer;
import how.hollow.utils.GCSUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class GCSAnnouncementWatcher  implements HollowConsumer.AnnouncementWatcher {

    private static Logger log = Logger.getLogger("GCSAnnouncementWatcher");

    private long latestVersion;
    private String bucket;
    private String folder;
    private String versionBlobName = "/version";
    private String logPrefix;
    private Storage storage;
    private boolean subscribedToUpdates;
    private final List<HollowConsumer> subscribedConsumers;

    GCSAnnouncementWatcher(String domain, String bucketName, String namespace, String applicationName, boolean subscribeToUpdates) {
        bucket = bucketName;
        folder = GCSUtils.getFolder(domain, namespace);
        logPrefix = "(" + folder + ") ";
        storage = GCSUtils.getStorage(applicationName);
        this.subscribedConsumers = Collections.synchronizedList(new ArrayList<HollowConsumer>());
        latestVersion = readLatestVersion();
        subscribedToUpdates = subscribeToUpdates;
        setupPollingThread();
    }

    @Override
    public long getLatestVersion() {
        return latestVersion;
    }

    @Override
    public void subscribeToUpdates(HollowConsumer consumer) {
        if(subscribedToUpdates)
            subscribedConsumers.add(consumer);
    }

    void setupPollingThread() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        long currentVersion = readLatestVersion();
                        if(latestVersion != currentVersion) {
                            latestVersion = currentVersion;
                            for(HollowConsumer consumer : subscribedConsumers) {
                                log.info("trigger refresh for version $latestVersion");
                                consumer.triggerAsyncRefresh();
                            }
                        }

                        Thread.sleep(1000 );
                    } catch(Throwable th) {
                        th.printStackTrace();
                    }
                }
            }
        });

        t.setName("hollow-google-cloud-storage-announcementwatcher-poller");
        t.setDaemon(true);
        t.start();
    }

    private long readLatestVersion() {
        String blobName = folder  + versionBlobName;
        try {
            StorageRequester storageRequester = getStorageRequester(blobName);
            return Long.parseLong(IOUtils.toString(storageRequester.getInputStream(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warning(folder + " error getting latest version");
            throw new RuntimeException("error getting latest version ", e);
        }
    }

    private StorageRequester getStorageRequester(String blobName) throws IOException {
        return new StorageRequester(storage.objects().get(bucket, blobName));
    }
}

package how.hollow.consumer.infrastructure;

import com.amazonaws.auth.AWSCredentials;
import com.netflix.hollow.api.consumer.HollowConsumer;

public class S3StateRetriever extends HollowConsumer.BlobStoreStateRetriever {
    public S3StateRetriever(AWSCredentials credentials, String bucketName, String blobNamespace) {
        super(new S3AnnouncementWatcher(credentials, bucketName, blobNamespace), new S3BlobRetriever(credentials, bucketName, blobNamespace));
    }
}

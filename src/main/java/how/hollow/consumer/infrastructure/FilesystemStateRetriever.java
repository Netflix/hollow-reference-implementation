package how.hollow.consumer.infrastructure;

import static how.hollow.producer.util.ScratchPaths.makePublishDir;

import java.nio.file.NoSuchFileException;

import com.netflix.hollow.api.consumer.HollowConsumer;

public class FilesystemStateRetriever extends HollowConsumer.BlobStoreStateRetriever {
    public FilesystemStateRetriever(String namespace) {
        super(new FilesystemAnnouncementWatcher(makePublishDir(namespace)) {
            @Override
            public long readLatestVersion() {
                try {
                    return super.readLatestVersion();
                } catch(RuntimeException ex) {
                    // TODO: timt: a kludge
                    if(ex.getCause() instanceof NoSuchFileException) return Long.MIN_VALUE;
                    else throw ex;
                }
            }
        }, new FilesystemBlobRetriever(namespace));
    }
}

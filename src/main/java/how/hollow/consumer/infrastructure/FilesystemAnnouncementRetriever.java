package how.hollow.consumer.infrastructure;

import static how.hollow.producer.infrastructure.FilesystemAnnouncer.ANNOUNCEMENT_FILENAME;
import static how.hollow.producer.util.ScratchPaths.makePublishDir;
import static java.nio.file.Files.newBufferedReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.netflix.hollow.api.consumer.HollowConsumer;

public class FilesystemAnnouncementRetriever implements HollowConsumer.AnnouncementRetriever {

    private Path publishDir;

    public FilesystemAnnouncementRetriever(String namespace) {
        this(makePublishDir(namespace));
    }

    public FilesystemAnnouncementRetriever(Path publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public long get() {
        long result = Long.MIN_VALUE;
        Path announcementPath = publishDir.resolve(ANNOUNCEMENT_FILENAME);
        if(Files.exists(announcementPath)) {
            try(BufferedReader reader = newBufferedReader(announcementPath)) {
                result = Long.parseLong(reader.readLine());
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

}

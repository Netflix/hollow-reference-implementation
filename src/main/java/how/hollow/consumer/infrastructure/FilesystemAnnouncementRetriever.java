package how.hollow.consumer.infrastructure;

import static how.hollow.producer.infrastructure.FilesystemAnnouncer.ANNOUNCEMENT_FILENAME;
import static java.nio.file.Files.newBufferedReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;

import com.netflix.hollow.api.consumer.HollowAnnouncementRetriever;

public class FilesystemAnnouncementRetriever implements HollowAnnouncementRetriever {

    private Path publishDir;

    public FilesystemAnnouncementRetriever(Path publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public long get() {
        Path announcementPath = publishDir.resolve(ANNOUNCEMENT_FILENAME);
        try(BufferedReader reader = newBufferedReader(announcementPath)) {
            return Long.parseLong(reader.readLine());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

}

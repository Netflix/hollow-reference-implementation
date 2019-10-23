/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package how.hollow.producer;

import com.netflix.hollow.api.consumer.HollowConsumer.AnnouncementWatcher;
import com.netflix.hollow.api.consumer.HollowConsumer.BlobRetriever;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Announcer;
import com.netflix.hollow.api.producer.HollowProducer.Publisher;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import how.hollow.producer.datamodel.Movie;
import how.hollow.producer.datamodel.SourceDataRetriever;
import java.io.File;


public class Producer {
    
    public static final String SCRATCH_DIR = System.getProperty("java.io.tmpdir");
    private static final long MIN_TIME_BETWEEN_CYCLES = 10000;

    public static void main(String args[]) {
        File publishDir = new File(SCRATCH_DIR, "publish-dir");
        publishDir.mkdir();
        
        System.out.println("I AM THE PRODUCER.  I WILL PUBLISH TO " + publishDir.getAbsolutePath());
        
        Publisher publisher = new HollowFilesystemPublisher(publishDir.toPath());
        Announcer announcer = new HollowFilesystemAnnouncer(publishDir.toPath());
        
        BlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishDir.toPath());
        AnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishDir.toPath());
        
        HollowProducer producer = HollowProducer.withPublisher(publisher)
                                                .withAnnouncer(announcer)
                                                .build();
        
        producer.initializeDataModel(Movie.class);
        
        restoreIfAvailable(producer, blobRetriever, announcementWatcher);
        
        cycleForever(producer);

    }

    public static void restoreIfAvailable(HollowProducer producer, 
            BlobRetriever retriever, 
            AnnouncementWatcher unpinnableAnnouncementWatcher) {

        System.out.println("ATTEMPTING TO RESTORE PRIOR STATE...");
        long latestVersion = unpinnableAnnouncementWatcher.getLatestVersion();
        if(latestVersion != AnnouncementWatcher.NO_ANNOUNCEMENT_AVAILABLE) {
            producer.restore(latestVersion, retriever);
            System.out.println("RESTORED " + latestVersion);
        } else {
            System.out.println("RESTORE NOT AVAILABLE");
        }
    }
    
    
    public static void cycleForever(HollowProducer producer) {
        final SourceDataRetriever sourceDataRetriever = new SourceDataRetriever();
        
        long lastCycleTime = Long.MIN_VALUE;
        while(true) {
            waitForMinCycleTime(lastCycleTime);
            lastCycleTime = System.currentTimeMillis();
            producer.runCycle(writeState -> {
                for(Movie movie : sourceDataRetriever.retrieveAllMovies()) {
                    writeState.add(movie);  /// <-- this is thread-safe, and can be done in parallel
                }
            });
        }
    }

    private static void waitForMinCycleTime(long lastCycleTime) {
        long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;
        
        while(System.currentTimeMillis() < targetNextCycleTime) {
            try {
                Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
            } catch(InterruptedException ignore) { }
        }
    }

}

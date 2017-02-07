package com.netflix.hollow.example;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.netflix.hollow.api.StateTransition;
import com.netflix.hollow.api.client.HollowClient;
import com.netflix.hollow.api.producer.HollowAnnouncer;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowPublisher;
import com.netflix.hollow.api.producer.VersionMinter;
import com.netflix.hollow.api.producer.WriteState;

import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import how.hollow.producer.infrastructure.FilesystemAnnouncer;
import how.hollow.producer.infrastructure.FilesystemPublisher;
import how.hollow.producer.util.DataMonkey;
import how.hollow.producer.util.VersionMinterWithCounter;

public class ExampleProducer {
    public static final String SCRATCH_DIR = System.getProperty("java.io.tmpdir");
    private static final long MIN_TIME_BETWEEN_CYCLES = SECONDS.toMillis(10);
    private HollowProducer hollow;

    public static void main(String args[]) throws InterruptedException {
        File workDir = new File(SCRATCH_DIR, "work-dir");
        File publishDir = new File(SCRATCH_DIR, "publish-dir");
        workDir.mkdirs();
        publishDir.mkdirs();

        System.out.format("I AM THE PRODUCER.\nI WORK IN %s\nI WILL PUBLISH TO %s\n", workDir.getAbsolutePath(), publishDir.getAbsolutePath());

        VersionMinter versionMinter = new VersionMinterWithCounter();
        HollowPublisher publisher = new FilesystemPublisher(workDir, publishDir);
        HollowAnnouncer announcer = new FilesystemAnnouncer(publishDir);

        /// randomly perturb the source data each cycle so that deltas will be produced
        final DataMonkey monkey = new DataMonkey();

        ExampleProducer producer = new ExampleProducer(new HollowProducer(versionMinter, publisher, announcer));
        producer.restoreIfAvailable(publishDir);
        producer.cycleForever(new HollowProducer.Task(){
            @Override
            public void populate(WriteState newState) {
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(monkey.ook(new Actor(263, "Henry Thomas")));
                    cast.add(monkey.ook(new Actor(337, "Drew Barrymore")));
                    Movie movie = monkey.ook(new Movie(37, "E.T. the Extra-Terrestrial", cast));

                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(monkey.ook(new Actor(337, "Drew Barrymore")));
                    Movie movie = monkey.ook(new Movie(193, "Firestarter", cast));

                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(monkey.ook(new Actor(2777, "Finn Wolfhard")));
                    cast.add(monkey.ook(new Actor(11, "Millie Bobby Brown")));
                    cast.add(monkey.ook(new Actor(953, "Gaten Matarazzo")));
                    cast.add(monkey.ook(new Actor(3137, "Caleb McLaughlin")));
                    Movie movie = monkey.ook(new Movie(1987, "Stranger Things Season 1", cast));

                    newState.add(movie);
                }
            }
        });

    }

    ExampleProducer(HollowProducer hollowProducer) {
        this.hollow = hollowProducer;
    }

    public void restoreIfAvailable(File publishDir) {
        System.out.println("RESTORE PRIOR STATE...");
        try {
            StateTransition latest = new FilesystemAnnouncementWatcher(publishDir).readLatestVersion();

            HollowClient client = new HollowClient(new FilesystemBlobRetriever(publishDir));
            client.triggerRefreshTo(latest.getToVersion());

            hollow.restoreFrom(client.getStateEngine(), client.getCurrentVersionId());
            System.out.format("RESUMING DELTA CHAIN AT %s\n", latest);
        } catch(Exception e) {
            System.out.println("RESTORE UNAVAILABLE; PRODUCING NEW DELTA CHAIN");
        }
    }

    public void cycleForever(HollowProducer.Task task) {
        long lastCycleTime = Long.MIN_VALUE;
        while(true) {
            waitForMinCycleTime(lastCycleTime);
            lastCycleTime = System.currentTimeMillis();
            hollow.runCycle(task);
        }
    }

    private void waitForMinCycleTime(long lastCycleTime) {
        long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;

        while(System.currentTimeMillis() < targetNextCycleTime) {
            try {
                Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
            } catch(InterruptedException ignore) { }
        }
    }
}

package how.hollow;

import static how.hollow.producer.util.ScratchPaths.makeProductDir;
import static how.hollow.producer.util.ScratchPaths.makePublishDir;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Populator;
import com.netflix.hollow.api.producer.HollowProducer.WriteState;

import how.hollow.consumer.infrastructure.FilesystemAnnouncementRetriever;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import how.hollow.producer.infrastructure.FilesystemAnnouncer;
import how.hollow.producer.infrastructure.FilesystemPublisher;
import how.hollow.producer.util.DataMonkey;
import how.hollow.producer.util.VersionMinterWithCounter;

public class CyclicProducer {
    public static void main(String args[]) throws InterruptedException {

        String namespace = args.length == 0 ? "cyclic" : args[0];

        Path productDir = makeProductDir(namespace);
        Path publishDir = makePublishDir(namespace);

        out.format("I AM THE PRODUCER\n  PRODUCING IN  %s\n  PUBLISHING TO %s\n", productDir, publishDir);

        final DataMonkey monkey = new DataMonkey(); /// simulate source data changing over time

        HollowProducer hollowProducer = new HollowProducer(
                new VersionMinterWithCounter(),
                new FilesystemPublisher(productDir, publishDir),
                new FilesystemAnnouncer(publishDir),
                new FilesystemAnnouncementRetriever(publishDir),
                new FilesystemBlobRetriever(publishDir));

        hollowProducer.initializeDataModel(Movie.class);
        hollowProducer.restore();    

        new CyclicProducer(hollowProducer).cycleForever(new Populator(){
            @Override
            public void populate(WriteState newState) {
                newState = monkey.introduceChaos(newState);
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(263, "Henry Thomas"));
                    cast.add(new Actor(337, "Drew Barrymore"));
                    Movie movie = new Movie(37, "E.T. the Extra-Terrestrial", cast);

                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(337, "Drew Barrymore"));
                    Movie movie = new Movie(193, "Firestarter", cast);

                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(2777, "Finn Wolfhard"));
                    cast.add(new Actor(11, "Millie Brown"));
                    cast.add(new Actor(953, "Gaten Matarazzo"));
                    cast.add(new Actor(3137, "Caleb McLaughlin"));
                    Movie movie = new Movie(1987, "Stranger Things Season 1", cast);

                    newState.add(movie);
                }
            }
        });

    }

    private final HollowProducer hollowProducer;

    CyclicProducer(HollowProducer hollowProducer) {
        this.hollowProducer = hollowProducer;
    }

    public void cycleForever(HollowProducer.Populator task) {
        long lastCycleTime = Long.MIN_VALUE;
        while(true) {
            waitForMinCycleTime(lastCycleTime);
            lastCycleTime = System.currentTimeMillis();
            hollowProducer.runCycle(task);
        }
    }

    private static final long MIN_TIME_BETWEEN_CYCLES = SECONDS.toMillis(10);

    private void waitForMinCycleTime(long lastCycleTime) {
        long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;

        while(System.currentTimeMillis() < targetNextCycleTime) {
            try {
                Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
            } catch(InterruptedException ignore) { }
        }
    }
}

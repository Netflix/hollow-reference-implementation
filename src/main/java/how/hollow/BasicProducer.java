package how.hollow;

import static how.hollow.producer.util.ScratchPaths.makeProductDir;
import static how.hollow.producer.util.ScratchPaths.makePublishDir;
import static java.lang.System.out;

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
import how.hollow.producer.util.VersionMinterWithCounter;

public class BasicProducer {
    public static void main(String args[]) throws InterruptedException {

        String namespace = args.length == 0 ? "basic" : args[0];

        Path productDir = makeProductDir(namespace);
        Path publishDir = makePublishDir(namespace);

        out.format("I AM THE PRODUCER\n  PRODUCING IN  %s\n  PUBLISHING TO %s\n", productDir, publishDir);

        HollowProducer hollowProducer = new HollowProducer(
                new VersionMinterWithCounter(),
                new FilesystemPublisher(productDir, publishDir),
                new FilesystemAnnouncer(publishDir),
                new FilesystemAnnouncementRetriever(publishDir),
                new FilesystemBlobRetriever(publishDir));

        /// 1. Initialize your data model
        hollowProducer.initializeDataModel(Movie.class);
        
        /// 2. Restore from prevous announced state to resume the delta chain
        hollowProducer.restore();        

        /// 3. Run one cycle, populating the new state with your data model
        hollowProducer.runCycle(new Populator(){
            @Override
            public void populate(WriteState newState) {
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
                    cast.add(new Actor(11, "Millie Bobby Brown"));
                    cast.add(new Actor(953, "Gaten Matarazzo"));
                    cast.add(new Actor(3137, "Caleb McLaughlin"));
                    Movie movie = new Movie(1987, "Stranger Things Season 1", cast);

                    newState.add(movie);
                }
            }
        });
        
        /// 4. Change the values for one actor above; run again to see a delta produced
        
        out.println("CIAO!");
    }

}

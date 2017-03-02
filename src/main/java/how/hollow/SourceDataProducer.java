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
package how.hollow;

import static how.hollow.producer.util.ScratchPaths.makeProductDir;
import static how.hollow.producer.util.ScratchPaths.makePublishDir;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Populator;
import com.netflix.hollow.api.producer.HollowProducer.WriteState;
import com.netflix.hollow.api.producer.HollowProducerListener;

import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import how.hollow.producer.infrastructure.FilesystemAnnouncer;
import how.hollow.producer.infrastructure.FilesystemPublisher;
import how.hollow.producer.sourcedata.SourceData;
import how.hollow.producer.util.DataMonkey;
import how.hollow.producer.util.StandardStreamsLogger;

public class SourceDataProducer {
    public static void main(String args[]) throws InterruptedException {

        /// 1. Start the producer; open `MovieData.txt` and `ActorData.txt` files in an editor

        SourceDataProducer myProducer = new SourceDataProducer(args.length == 0 ? "sourcedata" : args[0])
                .initializeDataModel(Movie.class)
                .restore();

        /// 2. Edit movie or actor rows in the "database"; save; observe a delta; repeat

        myProducer.cycleForever(new Populator(){
            @Override
            public void populate(WriteState newState) {

                /// 2b. uncomment the `dataMonkey` line below to simulate source data changes automatically

                /// DataMonkey(TM) for demonstration purposes only; do NOT taunt DataMonkey
                //newState = dataMonkey.introduceChaos(newState);

                try(SourceData movieData = new SourceData("MovieData.txt");
                        SourceData actorData = new SourceData("ActorData.txt")) {
                    for(SourceData.Row movieRow : movieData) {
                        Iterator<SourceData.Column> movieColumns = movieRow.iterator();

                        String title = movieColumns.next().value;
                        int releaseYear = movieColumns.next().toInt();
                        Set<Integer> actorIds = movieColumns.next().toIds();
                        Set<Actor> movieActors = new LinkedHashSet<>();
                        for(int actorId : actorIds) {
                            SourceData.Row actorRow = actorData.get(actorId);
                            Iterator<SourceData.Column> actorColumns = actorRow.iterator();
                            String actorName = actorColumns.next().value;
                            Actor actor = new Actor(actorId, actorName);

                            //if(actor.actorId == 20001751) throw new RuntimeException("boom!");
                            movieActors.add(actor);
                        }
                        Movie movie = new Movie(movieRow.id, title, releaseYear, movieActors);

                        newState.add(movie);
                    }
                }
            }
        });

        /// BONUS: create a producer that ingests data from your source-of-truth. See if you
        ///        can avoid having your entire source data loaded in memory at the same time.
    }

    public SourceDataProducer(String namespace) {
        final Path productDir = makeProductDir(namespace);
        this.publishDir = makePublishDir(namespace);

        HollowProducer hollowProducer = new HollowProducer(
                new FilesystemPublisher(productDir, publishDir),
                new FilesystemAnnouncer(publishDir));

        HollowProducerListener logger = new StandardStreamsLogger(){
            @Override public void onProducerInit(long elapsed, TimeUnit unit) {
                info("I AM THE PRODUCER\n  PRODUCING IN  %s\n  PUBLISHING TO %s\n", productDir, publishDir);
                super.onProducerInit(elapsed, unit);
            }
        };
        hollowProducer.addListener(logger);

        this.hollowProducer = hollowProducer;
    }

    public SourceDataProducer initializeDataModel(Class<?>...classes) {
        hollowProducer.initializeDataModel(classes);
        return this;
    }

    public SourceDataProducer restore() {
        FilesystemAnnouncementWatcher announcements = new FilesystemAnnouncementWatcher(publishDir);
        FilesystemBlobRetriever blobRetriever = new FilesystemBlobRetriever(publishDir);
        hollowProducer.restore(announcements.readLatestVersion(), blobRetriever);
        return this;
    }

    public void cycleForever(HollowProducer.Populator task) {
        long lastCycleTime = Long.MIN_VALUE;
        while(true) {
            waitForMinCycleTime(lastCycleTime);
            lastCycleTime = System.currentTimeMillis();
            hollowProducer.runCycle(task);
        }
    }

    @SuppressWarnings("unused")
    private static final DataMonkey dataMonkey = new DataMonkey();
    private static final long MIN_TIME_BETWEEN_CYCLES = SECONDS.toMillis(10);

    private final HollowProducer hollowProducer;
    private final Path publishDir;

    private void waitForMinCycleTime(long lastCycleTime) {
        long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;

        while(System.currentTimeMillis() < targetNextCycleTime) {
            try {
                Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
            } catch(InterruptedException ignore) { }
        }
    }
}

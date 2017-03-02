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

import java.util.HashSet;
import java.util.Set;

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Populator;
import com.netflix.hollow.api.producer.HollowProducer.WriteState;

import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import how.hollow.producer.infrastructure.FilesystemAnnouncer;
import how.hollow.producer.infrastructure.FilesystemPublisher;

public class BasicProducer {
    public static void main(String args[]) throws InterruptedException {

        String namespace = args.length == 0 ? "basic" : args[0];

        FilesystemPublisher publisher = new FilesystemPublisher(namespace);
        HollowProducer hollowProducer = new HollowProducer(
                publisher,
                new FilesystemAnnouncer(namespace));

        /// 1. Initialize your data model
        hollowProducer.initializeDataModel(Movie.class);

        /// 2. Restore from the previous announced state to resume the delta chain
        FilesystemAnnouncementWatcher announcements = new FilesystemAnnouncementWatcher(publisher.getPublishDir());
        FilesystemBlobRetriever blobRetriever = new FilesystemBlobRetriever(publisher.getPublishDir());
        hollowProducer.restore(announcements.readLatestVersion(), blobRetriever);

        /// 3. Run one cycle, populating the new state with your source data
        hollowProducer.runCycle(new Populator(){
            @Override
            public void populate(WriteState newState) {
                /// 3a. Mint POJOs on the fly, add them to the new state, then release them to be GC'd
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(263, "Henry Thomas"));
                    cast.add(new Actor(337, "Drew Barrymore"));
                    Movie movie = new Movie(37, "E.T. the Extra-Terrestrial", 1982, cast);

                    /// 3b. only the top-level (or root) instances need be added; Hollow does the rest
                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(337, "Drew Barrymore"));
                    Movie movie = new Movie(193, "Firestarter", 1984, cast);

                    newState.add(movie);
                }
                {
                    Set<Actor> cast = new HashSet<>();
                    cast.add(new Actor(2777, "Finn Wolfhard"));
                    cast.add(new Actor(11, "Millie Bobby Brown"));
                    cast.add(new Actor(953, "Gaten Matarazzo"));
                    cast.add(new Actor(3137, "Caleb McLaughlin"));
                    Movie movie = new Movie(1987, "Stranger Things Season 1", 2016, cast);

                    newState.add(movie);
                }
            }
        });

        /*
         * 4. Start the reference consumer with the "basic" namespace; explore the data
         *
         * 5. Change some hard-coded movie & actor values above; run again to see a delta produced and consumed
         *    moments later.
         *
         * 5a. Try commenting out a `cast.add(...)` or a `newState.add(movie)` line and running a cycle.
         *
         * UP NEXT: `CyclicProducer` demonstrates a producer running on a regular cadence
         *
         * BONUS: try adding and populating a new field in the Actor type
         */
    }
}

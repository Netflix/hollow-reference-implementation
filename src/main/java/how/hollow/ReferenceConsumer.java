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

import static java.lang.System.out;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.netflix.hollow.api.client.HollowAnnouncementWatcher;
import com.netflix.hollow.api.client.HollowBlobRetriever;
import com.netflix.hollow.api.client.HollowClient;
import com.netflix.hollow.api.client.HollowClientMemoryConfig;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;

import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.MovieAPIFactory;
import how.hollow.consumer.history.ConsumerHistoryListener;
import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;

public class ReferenceConsumer {

    public static void main(String args[]) throws Exception {
        String namespace = args.length == 0 ? "basic" : args[0];
        Path publishDir = Paths.get(System.getProperty("java.io.tmpdir"), namespace, "published");

        out.println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir);

        HollowBlobRetriever blobRetriever = new FilesystemBlobRetriever(namespace, publishDir);
        HollowAnnouncementWatcher announcementWatcher = new FilesystemAnnouncementWatcher(publishDir);

        ReferenceConsumer consumer = new ReferenceConsumer(blobRetriever, announcementWatcher);

        HollowHistoryUIServer historyUIServer = new HollowHistoryUIServer(consumer.historyListener.getHistory(), 7777);
        historyUIServer.start();
        historyUIServer.join();
    }

    private final HollowClient client;
    private final ConsumerHistoryListener historyListener;

    public ReferenceConsumer(HollowBlobRetriever blobRetriever, HollowAnnouncementWatcher announcementWatcher) {
        this.historyListener = new ConsumerHistoryListener();

        this.client = new HollowClient(
                blobRetriever, 
                announcementWatcher, 
                historyListener, 
                new MovieAPIFactory(), 
                HollowClientMemoryConfig.DEFAULT_CONFIG);

        client.triggerRefresh();
    }

    public MovieAPI getAPI() {
        return (MovieAPI) client.getAPI();
    }

    public HollowReadStateEngine getStateEngine() {
        return client.getStateEngine();
    }

}

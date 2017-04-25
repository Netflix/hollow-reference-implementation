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
package how.hollow.consumer;

import com.netflix.hollow.api.client.HollowAnnouncementWatcher;
import com.netflix.hollow.api.client.HollowBlobRetriever;
import com.netflix.hollow.api.client.HollowClient;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.history.ConsumerHistoryListener;
import how.hollow.consumer.infrastructure.FilesystemAnnouncementWatcher;
import how.hollow.consumer.infrastructure.FilesystemBlobRetriever;
import how.hollow.producer.Producer;
import java.io.File;

public class Consumer {
    
    private final HollowClient client;
    private final ConsumerHistoryListener historyListener;
    
    public Consumer(HollowBlobRetriever blobRetriever, HollowAnnouncementWatcher announcementWatcher) {
    	this.historyListener = new ConsumerHistoryListener();
    	
        this.client = new HollowClient.Builder()
                .withBlobRetriever(blobRetriever)
                .withAnnouncementWatcher(announcementWatcher)
                .withUpdateListener(historyListener)
                .withGeneratedAPIClass(MovieAPI.class)
                .build();
    }
    
    public void initialize() {
        client.triggerRefresh();
    }
    
    public MovieAPI getAPI() {
        return (MovieAPI) client.getAPI();
    }
    
    public HollowReadStateEngine getStateEngine() {
        return client.getStateEngine();
    }
    
    public static void main(String args[]) throws Exception {
        File publishDir = new File(Producer.SCRATCH_DIR, "publish-dir");
        
        System.out.println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir.getAbsolutePath());

        HollowBlobRetriever blobRetriever = new FilesystemBlobRetriever(publishDir);
        HollowAnnouncementWatcher announcementWatcher = new FilesystemAnnouncementWatcher(publishDir);
        
        Consumer consumer = new Consumer(blobRetriever, announcementWatcher);
        consumer.initialize();
        
        /// example usage of client API:
        MovieAPI api = (MovieAPI)consumer.client.getAPI();
        System.out.println("THERE ARE " + api.getAllMovieHollow().size() + " MOVIES IN THE DATASET AT STARTUP");

        /// start a Hollow History UI server (point your browser to http://localhost:7777)
        HollowHistoryUIServer historyUIServer = new HollowHistoryUIServer(consumer.historyListener.getHistory(), 7777);
        historyUIServer.start();
        
        /// start a Hollow Explorer UI server (point your browser to http://localhost:7778)
        HollowExplorerUIServer explorerUIServer = new HollowExplorerUIServer(consumer.client, 7778);
        explorerUIServer.start();
        
        explorerUIServer.join();
    }
    
}

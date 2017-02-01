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
package com.netflix.hollow.example.consumer;

import com.netflix.hollow.api.client.HollowAnnouncementWatcher;
import com.netflix.hollow.api.client.HollowBlobRetriever;
import com.netflix.hollow.api.client.HollowClient;
import com.netflix.hollow.api.client.HollowClientMemoryConfig;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.example.consumer.api.generated.MovieAPI;
import com.netflix.hollow.example.consumer.api.generated.MovieAPIFactory;
import com.netflix.hollow.example.consumer.history.ConsumerHistoryListener;
import com.netflix.hollow.example.consumer.infrastructure.FilesystemAnnouncementWatcher;
import com.netflix.hollow.example.consumer.infrastructure.FilesystemBlobRetriever;
import com.netflix.hollow.example.producer.Producer;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import java.io.File;

public class Consumer {
    
    private final HollowClient client;
    private final ConsumerHistoryListener historyListener;
    
    public Consumer(HollowBlobRetriever blobRetriever, HollowAnnouncementWatcher announcementWatcher) {
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
    
    public static void main(String args[]) throws Exception {
        File publishDir = new File(Producer.SCRATCH_DIR, "publish-dir");
        
        System.out.println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir.getAbsolutePath());

        HollowBlobRetriever blobRetriever = new FilesystemBlobRetriever(publishDir);
        HollowAnnouncementWatcher announcementWatcher = new FilesystemAnnouncementWatcher(publishDir);
        
        Consumer consumer = new Consumer(blobRetriever, announcementWatcher);
        
        HollowHistoryUIServer historyUIServer = new HollowHistoryUIServer(consumer.historyListener.getHistory(), 7777);
        historyUIServer.start();
        historyUIServer.join();
    }
    
}

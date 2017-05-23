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
package how.hollow.consumer.infrastructure;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.HollowConsumer.AnnouncementWatcher;
import how.hollow.producer.infrastructure.S3Announcer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class S3AnnouncementWatcher implements AnnouncementWatcher {
    
    private final AmazonS3 s3;
    private final String bucketName;
    private final String blobNamespace;
    
    private final List<HollowConsumer> subscribedConsumers;

    private long latestVersion;
    
    
    public S3AnnouncementWatcher(AWSCredentials credentials, String bucketName, String blobNamespace) {
        this.s3 = new AmazonS3Client(credentials);
        this.bucketName = bucketName;
        this.blobNamespace = blobNamespace;
        this.subscribedConsumers = Collections.synchronizedList(new ArrayList<HollowConsumer>());
        
        this.latestVersion = readLatestVersion();

        setupPollingThread();
    }
    
    public void setupPollingThread() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        long currentVersion = readLatestVersion();
                        if(latestVersion != currentVersion) {
                            latestVersion = currentVersion;
                            for(HollowConsumer consumer : subscribedConsumers)
                                consumer.triggerAsyncRefresh();
                        }
                        
                        Thread.sleep(1000);
                    } catch(Throwable th) {
                        th.printStackTrace();
                    }
                }
            }
        });
        
        t.setName("hollow-s3-announcementwatcher-poller");
        t.setDaemon(true);
        t.start();
    }

    @Override
    public long getLatestVersion() {
        return latestVersion;
    }

    @Override
    public void subscribeToUpdates(HollowConsumer consumer) {
        subscribedConsumers.add(consumer);
    }
    
    
    private long readLatestVersion() {
        return Long.parseLong(s3.getObjectAsString(bucketName, blobNamespace + "/" + S3Announcer.ANNOUNCEMENT_OBJECTNAME));
    }
    

}

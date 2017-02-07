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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.netflix.hollow.api.StateTransition;
import com.netflix.hollow.api.client.HollowAnnouncementWatcher;

import how.hollow.producer.infrastructure.FilesystemAnnouncer;

public class FilesystemAnnouncementWatcher extends HollowAnnouncementWatcher {

    private final File publishDir;
    
    private StateTransition latest;
    
    public FilesystemAnnouncementWatcher(File publishDir) {
        this.publishDir = publishDir;
        this.latest = readLatestVersion();
    }
    
    @Override
    public long getLatestVersion() {
        return latest.getToVersion();
    }

    public StateTransition getLatest() {
        return latest;
    }

    @Override
    public void subscribeToEvents() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        StateTransition currentVersion = readLatestVersion();
                        if(latest.getToVersion() != currentVersion.getToVersion()) {
                            latest = currentVersion;
                            triggerAsyncRefresh();
                        }
                        
                            Thread.sleep(1000);
                    } catch(Throwable th) { 
                        th.printStackTrace();
                    }
                }
            }
        });
        
        t.setDaemon(true);
        t.start();
    }
    
    public StateTransition readLatestVersion() {
        File f = new File(publishDir, FilesystemAnnouncer.ANNOUNCEMENT_FILENAME);
        
        try(BufferedReader reader = new BufferedReader(new FileReader(f))) {
            return new StateTransition(Long.parseLong(reader.readLine()));
        } catch(IOException e) {
        	throw new RuntimeException(e);
        }
    }

}

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

import static how.hollow.producer.infrastructure.FilesystemAnnouncer.ANNOUNCEMENT_FILENAME;
import static java.nio.file.Files.newBufferedReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;

import com.netflix.hollow.api.client.HollowAnnouncementWatcher;

public class FilesystemAnnouncementWatcher extends HollowAnnouncementWatcher {

    private final Path publishDir;

    private long latestVersion;

    public FilesystemAnnouncementWatcher(Path publishDir) {
        this.publishDir = publishDir;
        this.latestVersion = readLatestVersion();
    }

    @Override
    public long getLatestVersion() {
        return latestVersion;
    }

    @Override
    public void subscribeToEvents() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        long currentVersion = readLatestVersion();
                        if(latestVersion != currentVersion) {
                            latestVersion = currentVersion;
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

    public long readLatestVersion() {
        Path announcementPath = publishDir.resolve(ANNOUNCEMENT_FILENAME);
        try(BufferedReader reader = newBufferedReader(announcementPath)) {
            return Long.parseLong(reader.readLine());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

}

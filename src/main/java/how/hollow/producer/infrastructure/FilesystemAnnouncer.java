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
package how.hollow.producer.infrastructure;

import static how.hollow.producer.util.ScratchPaths.makePublishDir;

import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;

import com.netflix.hollow.api.producer.HollowProducer;

public class FilesystemAnnouncer implements HollowProducer.Announcer {

    public static final String ANNOUNCEMENT_FILENAME = "announced.version";
    
    private final Path publishDir;

    public FilesystemAnnouncer(String namespace) {
        this(makePublishDir(namespace));
    }

    public FilesystemAnnouncer(Path publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public void announce(long stateVersion) {
        Path annnouncementPath = publishDir.resolve(ANNOUNCEMENT_FILENAME);
        
        try (BufferedWriter writer = newBufferedWriter(annnouncementPath, CREATE, WRITE, TRUNCATE_EXISTING, DSYNC)){
            writer.write(String.valueOf(stateVersion));
        } catch(IOException ex) {
            throw new RuntimeException("Unable to write to announcement file", ex);
        }
    }

}

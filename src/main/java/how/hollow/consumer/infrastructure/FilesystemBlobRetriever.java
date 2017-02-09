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

import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.StandardOpenOption.READ;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.netflix.hollow.api.client.HollowBlob;
import com.netflix.hollow.api.client.HollowBlobRetriever;

public class FilesystemBlobRetriever implements HollowBlobRetriever {
    
    private final Path publishDir;
    
    public FilesystemBlobRetriever(Path publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public HollowBlob retrieveSnapshotBlob(long desiredVersion) {
        Path exactPath = publishDir.resolve("snapshot-" + desiredVersion);
        
        if(Files.exists(exactPath))
            return new FilesystemBlob(exactPath, desiredVersion);
        
        long maxVersionBeforeDesired = Long.MIN_VALUE;
        Path maxVersionBeforeDesiredPath = null;

        try(DirectoryStream<Path> paths = newDirectoryStream(publishDir, "snapshot-*")) {
            for(Path path : paths) {
                long toVersion = parseToVersion(path);
                if(toVersion < desiredVersion && toVersion > maxVersionBeforeDesired) {
                    maxVersionBeforeDesired = toVersion;
                    maxVersionBeforeDesiredPath = path;
                }
            }
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }

        if(maxVersionBeforeDesired > Long.MIN_VALUE)
            return new FilesystemBlob(maxVersionBeforeDesiredPath, maxVersionBeforeDesired);
        
        return null;
    }

    @Override
    public HollowBlob retrieveDeltaBlob(long fromVersion) {
        try(DirectoryStream<Path> paths = newDirectoryStream(publishDir, "delta-" + fromVersion + "-*")) {
            for(Path path : paths) {
                long toVersion = parseToVersion(path);
                return new FilesystemBlob(path, fromVersion, toVersion);
            }
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }

        return null;
    }

    @Override
    public HollowBlob retrieveReverseDeltaBlob(long fromVersion) {
        try(DirectoryStream<Path> paths = newDirectoryStream(publishDir, "reversedelta-" + fromVersion + "-*")) {
            for(Path path : paths) {
                long toVersion = parseToVersion(path);
                return new FilesystemBlob(path, fromVersion, toVersion);
            }
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }

        return null;
    }

    private long parseToVersion(Path path) {
        String filename = path.getFileName().toString();
        long version = Long.parseLong(filename.substring(filename.lastIndexOf("-") + 1));
        return version;
    }

    private static class FilesystemBlob extends HollowBlob {

        private final Path blobPath;

        public FilesystemBlob(Path snapshotPath, long toVersion) {
            super(toVersion);
            this.blobPath = snapshotPath;
        }

        public FilesystemBlob(Path deltaPath, long fromVersion, long toVersion) {
            super(fromVersion, toVersion);
            this.blobPath = deltaPath;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return new BufferedInputStream(newInputStream(blobPath, READ));
        }
    }

}

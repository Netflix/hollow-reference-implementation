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

import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.netflix.hollow.api.HollowStateTransition;
import com.netflix.hollow.api.producer.HollowBlob;
import com.netflix.hollow.api.producer.HollowPublisher;

public class FilesystemPublisher implements HollowPublisher {

    private final Path productDir;
    private final Path publishDir;

    public FilesystemPublisher(Path productDir, Path publishDir) {
        this.productDir = productDir;
        this.publishDir = publishDir;
    }

    @Override
    public HollowBlob openSnapshot(HollowStateTransition transition) {
        Path snapshotPath = productDir.resolve(String.format("snapshot-%d", transition.getToVersion()));
        return new FilesystemBlob(snapshotPath);
    }

    @Override
    public HollowBlob openDelta(HollowStateTransition transition) {
        return openDeltaBlob(transition, "delta");
    }

    @Override
    public HollowBlob openReverseDelta(HollowStateTransition transition) {
        return openDeltaBlob(transition.reverse(), "reversedelta");
    }

    @Override
    public void publish(HollowBlob blob) {
        publishBlob((FilesystemBlob)blob);
    }

    private HollowBlob openDeltaBlob(HollowStateTransition transition, String fileType) {
        Path deltaPath = productDir.resolve(String.format("%s-%d-%d",
                fileType,
                transition.getFromVersion(),
                transition.getToVersion()));
        return new FilesystemBlob(deltaPath);
    }

    private void publishBlob(FilesystemBlob blob) {
        try {
            Path source = blob.getProductPath();
            Path filename = source.getFileName();
            Path dest = publishDir.resolve(filename);
            Path intermediate = dest.resolveSibling(filename + ".incomplete");
            Files.copy(source, intermediate, REPLACE_EXISTING);
            Files.move(intermediate, dest, ATOMIC_MOVE);
        } catch(IOException ex) {
            throw new RuntimeException("Unable to publish file!", ex);
        }
    }

    private static final class FilesystemBlob implements HollowBlob {

        private final Path product;
        private BufferedOutputStream out;

        FilesystemBlob(Path product) {
            this.product = product;
        }

        @Override
        public OutputStream getOutputStream() {
            try {
                out = new BufferedOutputStream(newOutputStream(product));
                //out = new BufferedOutputStream(newOutputStream(product));
                return out;
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close() {
            if(out != null) {
                try {
                    out.close();
                    out = null;
                    deleteIfExists(product);
                } catch(IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        Path getProductPath() {
            return product;
        }
    }
}

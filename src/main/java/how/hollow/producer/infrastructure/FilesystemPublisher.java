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

import static how.hollow.producer.util.ScratchPaths.makeProductDir;
import static how.hollow.producer.util.ScratchPaths.makePublishDir;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.netflix.hollow.api.producer.HollowProducer;

// TODO: timt: introduce AbstractHollowPublisher into hollow for all filesystem staged publishers to extend (us, S3publisher)
public class FilesystemPublisher implements HollowProducer.Publisher {

    private final Path productDir;
    private final Path publishDir;

    public FilesystemPublisher(String namespace) {
        this(makeProductDir(namespace), makePublishDir(namespace));
    }

    public FilesystemPublisher(Path productDir, Path publishDir) {
        this.productDir = productDir;
        this.publishDir = publishDir;
    }

    @Override
    public HollowProducer.Blob openSnapshot(long version) {
        Path snapshotPath = productDir.resolve(String.format("snapshot-%d", version));
        return new FilesystemBlob(snapshotPath);
    }

    @Override
    public HollowProducer.Blob openDelta(long fromVersion, long toVersion) {
        return openDeltaBlob(fromVersion, toVersion, "delta");
    }

    @Override
    public HollowProducer.Blob openReverseDelta(long fromVersion, long toVersion) {
        return openDeltaBlob(toVersion, fromVersion, "reversedelta");
    }

    @Override
    public void publish(HollowProducer.Blob blob) {
        publishBlob((FilesystemBlob)blob);
    }

    private HollowProducer.Blob openDeltaBlob(long origin, long destination, String fileType) {
        Path deltaPath = productDir.resolve(String.format("%s-%d-%d",
                fileType,
                origin,
                destination));
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

    private static final class FilesystemBlob implements HollowProducer.Blob {

        private final Path product;
        private BufferedOutputStream out;
        private BufferedInputStream in;

        // TODO: timt: pull FilesystemBlob into Hollow
        FilesystemBlob(Path product) {
            this.product = product;
        }

        @Override
        public OutputStream getOutputStream() {
            try {
                out = new BufferedOutputStream(newOutputStream(product));
                return out;
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public InputStream getInputStream() {
            try {
                in = new BufferedInputStream(newInputStream(product));
                return in;
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close() {
            if(out != null) {
                try {
                    if(out != null) out.close();
                    out = null;
                    if(in != null) in.close();
                    in = null;
                    // FIXME: timt: need cleanup to occur after integrity check roundtrip
                    boolean cleanup = false;
                    if(cleanup) deleteIfExists(product);
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

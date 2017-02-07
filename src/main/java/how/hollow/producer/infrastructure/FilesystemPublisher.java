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



import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import com.netflix.hollow.api.StateTransition;
import com.netflix.hollow.api.producer.HollowBlob;
import com.netflix.hollow.api.producer.HollowPublisher;

public class FilesystemPublisher implements HollowPublisher {

    private final File productDir;
    private final File publishDir;

    public FilesystemPublisher(File productDir, File publishDir) {
        this.productDir = productDir;
        this.publishDir = publishDir;
    }

    @Override
    public HollowBlob openSnapshot(StateTransition transition) {
        String filename = String.format("snapshot-%d", transition.getToVersion());
        return new FilesystemBlob(new File(productDir, filename));
    }

    @Override
    public HollowBlob openDelta(StateTransition transition) {
        return openDeltaBlob(transition, "delta");
    }

    @Override
    public HollowBlob openReverseDelta(StateTransition transition) {
        return openDeltaBlob(transition.reverse(), "reversedelta");
    }

    @Override
    public void publish(HollowBlob blob) {
        publishBlob((FilesystemBlob)blob);
    }

    private HollowBlob openDeltaBlob(StateTransition transition, String fileType) {
        String filename = String.format("%s-%d-%d", fileType, transition.getFromVersion(), transition.getToVersion());
        return new FilesystemBlob(new File(productDir, filename));
    }

    private void publishBlob(FilesystemBlob blob) {
        copyFile(blob.getScratchFile(), blob.getPublishedFile(publishDir));
    }

    private void copyFile(File sourceFile, File destFile) {
        try(InputStream is = new FileInputStream(sourceFile);
                OutputStream os = new FileOutputStream(destFile)) {
            IOUtils.copy(is, os);
        } catch(IOException e) {
            throw new RuntimeException("Unable to publish file!", e);
        }
    }

    private static final class FilesystemBlob implements HollowBlob {

        private final File product;
        private BufferedOutputStream out;

        FilesystemBlob(File product) {
            this.product = product;
        }

        @Override
        public OutputStream getOutputStream() {
            try {
                out = new BufferedOutputStream(new FileOutputStream(product));
                return out;
            } catch(FileNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void finish() {
            closeOutputStream();
        }

        @Override
        public void close() {
            closeOutputStream();
            product.delete();
        }

        File getScratchFile() {
            return product;
        }

        File getPublishedFile(File publishDir) {
            return new File(publishDir, product.getName());
        }

        private void closeOutputStream() {
            if(out != null) {
                try {
                    out.close();
                } catch(IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
}

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
package com.netflix.hollow.example.producer.infrastructure;

import com.netflix.hollow.example.producer.Publisher;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;

public class FilesystemPublisher implements Publisher {

    private final File publishDir;
    
    public FilesystemPublisher(File publishDir) {
        this.publishDir = publishDir;
    }

    @Override
    public void publishSnapshot(File snapshotFile, long stateVersion) {
        File publishedFile = new File(publishDir, "snapshot-" + stateVersion);
        copyFile(snapshotFile, publishedFile);
    }

    @Override
    public void publishDelta(File deltaFile, long previousVersion, long currentVersion) {
        File publishedFile = new File(publishDir, "delta-" + previousVersion + "-" + currentVersion);
        copyFile(deltaFile, publishedFile);
    }

    @Override
    public void publishReverseDelta(File reverseDeltaFile, long previousVersion, long currentVersion) {
        File publishedFile = new File(publishDir, "reversedelta-" + currentVersion + "-" + previousVersion);
        copyFile(reverseDeltaFile, publishedFile);
    }
    
    private void copyFile(File sourceFile, File destFile) {
        try(
                InputStream is = new FileInputStream(sourceFile);
                OutputStream os = new FileOutputStream(destFile);
        ) {
            IOUtils.copy(is, os);
        } catch(IOException e) {
            throw new RuntimeException("Unable to publish file!", e);
        }
    }
    
    
    
}

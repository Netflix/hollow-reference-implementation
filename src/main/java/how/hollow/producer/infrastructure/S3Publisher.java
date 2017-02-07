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

import static how.hollow.producer.infrastructure.S3Blob.Kind.DELTA;
import static how.hollow.producer.infrastructure.S3Blob.Kind.REVERSE_DELTA;
import static how.hollow.producer.infrastructure.S3Blob.Kind.SNAPSHOT;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.netflix.hollow.api.StateTransition;
import com.netflix.hollow.api.producer.HollowBlob;
import com.netflix.hollow.api.producer.HollowPublisher;
import com.netflix.hollow.core.memory.encoding.VarInt;

public class S3Publisher implements HollowPublisher {

    private final AmazonS3 s3;
    private final TransferManager s3TransferManager;
    private final String bucketName;
    private final String blobNamespace;

    private final List<Long> snapshotIndex;
    private File scratchDir;

    public S3Publisher(AWSCredentials credentials, String bucketName, String blobNamespace, File scratchDir) {
        this.bucketName = bucketName;
        this.blobNamespace = blobNamespace;
        this.scratchDir = scratchDir;
        this.s3 = new AmazonS3Client(credentials);
        this.s3TransferManager = new TransferManager(s3);
        this.snapshotIndex = initializeSnapshotIndex();
    }

    @Override
    public HollowBlob openSnapshot(StateTransition transition) {
        return new S3Blob(SNAPSHOT, blobNamespace, scratchDir, transition);
    }

    @Override
    public HollowBlob openDelta(StateTransition transition) {
        return new S3Blob(DELTA, blobNamespace, scratchDir, transition);
    }

    @Override
    public HollowBlob openReverseDelta(StateTransition transition) {
        return new S3Blob(REVERSE_DELTA, blobNamespace, scratchDir, transition);
    }

    @Override
    public void publish(HollowBlob blob) {
        uploadBlob((S3Blob)blob);
    }

    private void uploadBlob(S3Blob s3Blob) {
        /// upload blob to S3
        try (InputStream is = new BufferedInputStream(new FileInputStream(s3Blob.file))) {
            Upload upload = s3TransferManager.upload(bucketName, s3Blob.getS3ObjectName(), is, s3Blob.getS3ObjectMetadata());
            upload.waitForCompletion();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        /// now we update the snapshot index
        if(s3Blob.isSnapshot()) updateSnapshotIndex(s3Blob.transition);
    }

    /////////////////////// BEGIN SNAPSHOT INDEX CODE ///////////////////////
    /*
     * We need an index over the available state versions for which snapshot blobs are available.
     * The S3Publisher stores that index as an object with a known key in S3.
     * The remainder of this class deals with maintaining that index.
     */

    public static String getSnapshotIndexObjectName(String blobNamespace) {
        return blobNamespace + "/snapshot.index";
    }

    /**
     * Write a list of all of the state versions to S3.
     * @param newVersion
     */
    private synchronized void updateSnapshotIndex(StateTransition transition) {
        /// insert the new version into the list
        int idx = Collections.binarySearch(snapshotIndex, transition.getToVersion());
        int insertionPoint = Math.abs(idx) - 1;
        snapshotIndex.add(insertionPoint, transition.getToVersion());

        /// build a binary representation of the list -- gap encoded variable-length integers
        byte[] idxBytes = buidGapEncodedVarIntSnapshotIndex();

        /// indicate the Content-Length
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("Content-Length", (long)idxBytes.length);

        /// upload the new file content.
        try(InputStream is = new ByteArrayInputStream(idxBytes)) {
            Upload upload = s3TransferManager.upload(bucketName, getSnapshotIndexObjectName(blobNamespace), is, metadata);

            upload.waitForCompletion();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encode the sorted list of all state versions as gap-encoded variable length integers.
     * @return
     */
    private byte[] buidGapEncodedVarIntSnapshotIndex() {
        int idx;
        byte[] idxBytes;
        idx = 0;
        long currentSnapshotId = snapshotIndex.get(idx++);
        long currentSnapshotIdGap = currentSnapshotId;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            while(idx < snapshotIndex.size()) {
                VarInt.writeVLong(os, currentSnapshotIdGap);

                long nextSnapshotId = snapshotIndex.get(idx++);
                currentSnapshotIdGap = nextSnapshotId - currentSnapshotId;
                currentSnapshotId = nextSnapshotId;
            }

            VarInt.writeVLong(os, currentSnapshotIdGap);

            idxBytes = os.toByteArray();
        } catch(IOException shouldNotHappen) {
            throw new RuntimeException(shouldNotHappen);
        }

        return idxBytes;
    }

    /**
     * Find all of the existing snapshots.
     */
    private List<Long> initializeSnapshotIndex() {
        List<Long> snapshotIdx = new ArrayList<Long>();

        ObjectListing listObjects = s3.listObjects(bucketName, SNAPSHOT.getS3ObjectPrefix(blobNamespace));

        for (S3ObjectSummary summary : listObjects.getObjectSummaries())
            addSnapshotStateId(summary, snapshotIdx);

        while (listObjects.isTruncated()) {
            listObjects = s3.listNextBatchOfObjects(listObjects);

            for (S3ObjectSummary summary : listObjects.getObjectSummaries())
                addSnapshotStateId(summary, snapshotIdx);
        }

        Collections.sort(snapshotIdx);

        return snapshotIdx;
    }

    private void addSnapshotStateId(S3ObjectSummary obj, List<Long> snapshotIdx) {
        String key = obj.getKey();
        try {
            snapshotIdx.add(Long.parseLong(key.substring(key.lastIndexOf("-") + 1)));
        } catch(NumberFormatException ignore) { }
    }

}

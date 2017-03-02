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

import static com.netflix.hollow.api.producer.HollowProducer.Blob.Type.*;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.size;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
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
import com.netflix.hollow.api.producer.HollowProducer.Blob.Type;
import com.netflix.hollow.api.producer.fs.AbstractHollowPublisher;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Blob;
import com.netflix.hollow.core.memory.encoding.HashCodes;
import com.netflix.hollow.core.memory.encoding.VarInt;

public class S3Publisher extends AbstractHollowPublisher {
    private final AmazonS3 s3;
    private final TransferManager s3TransferManager;
    private final String bucketName;

    private final List<Long> snapshotIndex;

    public S3Publisher(AWSCredentials credentials, String bucketName, String namespace, Path stagingPath) {
        super(namespace, stagingPath);
        this.bucketName = bucketName;
        this.s3 = new AmazonS3Client(credentials);
        this.s3TransferManager = new TransferManager(s3);
        this.snapshotIndex = initializeSnapshotIndex();
    }

    @Override
    public void publish(Blob blob) {
        uploadBlob((StagedBlob)blob);
    }

    private void uploadBlob(StagedBlob blob) {
        /// upload blob to S3
        try (InputStream is = new BufferedInputStream(newInputStream(blob.getStagedArtifactPath()))) {
            Upload upload = s3TransferManager.upload(bucketName,
                    getS3ObjectName(blob),
                    is,
                    getS3ObjectMetadata(blob));
            upload.waitForCompletion();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        /// now we update the snapshot index
        if(blob.getType() == SNAPSHOT) updateSnapshotIndex(blob);
    }


    ObjectMetadata getS3ObjectMetadata(StagedBlob blob) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader("Content-Length", size(blob.getStagedArtifactPath()));
            populateObjectMetadata(blob, metadata);
            return metadata;
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void populateObjectMetadata(StagedBlob blob, ObjectMetadata metadata) {
        switch(blob.getType()) {
        case SNAPSHOT:
            metadata.addUserMetadata("to_state", String.valueOf(blob.getToVersion()));
            break;
        case DELTA:
        case REVERSE_DELTA:
            populateDeltaMetadata(blob, metadata);
            break;
        default:
            throw new IllegalStateException("unknown blob type, type=" + blob.getType());
        }
    }

    private void populateDeltaMetadata(StagedBlob blob, ObjectMetadata metadata) {
        metadata.addUserMetadata("from_state", String.valueOf(blob.getFromVersion()));
        metadata.addUserMetadata("to_state", String.valueOf(blob.getToVersion()));
    }

    public String getS3ObjectPrefix(Blob.Type type) {
        return new StringBuilder(namespace)
                .append("/")
                .append(type.prefix)
                .append("/")
                .toString();
    }

    public String getS3ObjectName(Blob.Type type, long lookupVersion) {
        return new StringBuilder(getS3ObjectPrefix(type))
                .append(Integer.toHexString(HashCodes.hashLong(lookupVersion)))
                .append('-')
                .append(lookupVersion)
                .toString();
    }

    public String getS3ObjectName(StagedBlob blob) {
        return getS3ObjectName(blob.getType(), blob.getType() == SNAPSHOT ? blob.getToVersion() : blob.getFromVersion());
    }

    /////////////////////// BEGIN SNAPSHOT INDEX CODE ///////////////////////
    /*
     * We need an index over the available state versions for which snapshot blobs are available.
     * The S3Publisher stores that index as an object with a known key in S3.
     * The remainder of this class deals with maintaining that index.
     */

    public static String getSnapshotIndexObjectName(String namespace) {
        return namespace + "/snapshot.index";
    }

    /**
     * Write a list of all of the state versions to S3.
     * @param newVersion
     */
    private synchronized void updateSnapshotIndex(Blob blob) {
        /// insert the new version into the list
        int idx = Collections.binarySearch(snapshotIndex, blob.getToVersion());
        int insertionPoint = Math.abs(idx) - 1;
        snapshotIndex.add(insertionPoint, blob.getToVersion());

        /// build a binary representation of the list -- gap encoded variable-length integers
        byte[] idxBytes = buidGapEncodedVarIntSnapshotIndex();

        /// indicate the Content-Length
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("Content-Length", (long)idxBytes.length);

        /// upload the new file content.
        try(InputStream is = new ByteArrayInputStream(idxBytes)) {
            Upload upload = s3TransferManager.upload(bucketName, getSnapshotIndexObjectName(namespace), is, metadata);

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

        ObjectListing listObjects = s3.listObjects(bucketName, getS3ObjectPrefix(SNAPSHOT));

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

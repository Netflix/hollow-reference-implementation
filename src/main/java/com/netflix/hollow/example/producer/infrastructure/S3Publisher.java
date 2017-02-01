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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.netflix.hollow.core.memory.encoding.HashCodes;
import com.netflix.hollow.core.memory.encoding.VarInt;
import com.netflix.hollow.example.producer.Publisher;
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

public class S3Publisher implements Publisher {
    
    private final AmazonS3 s3;
    private final TransferManager s3TransferManager;
    private final String bucketName;
    private final String blobNamespace;
    
    private final List<Long> snapshotIndex;
    
    public S3Publisher(AWSCredentials credentials, String bucketName, String blobNamespace) {
        this.s3 = new AmazonS3Client(credentials);
        this.s3TransferManager = new TransferManager(s3);
        this.bucketName = bucketName;
        this.blobNamespace = blobNamespace;
        this.snapshotIndex = initializeSnapshotIndex();
    }

    @Override
    public void publishSnapshot(File snapshotFile, long stateVersion) {
        String objectName = getS3ObjectName(blobNamespace, "snapshot", stateVersion);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("to_state", String.valueOf(stateVersion));
        metadata.setHeader("Content-Length", snapshotFile.length());
        
        uploadFile(snapshotFile, objectName, metadata);
        
        /// now we update the snapshot index
        updateSnapshotIndex(stateVersion);
    }

    @Override
    public void publishDelta(File deltaFile, long previousVersion, long currentVersion) {
        String objectName = getS3ObjectName(blobNamespace, "delta", previousVersion);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("from_state", String.valueOf(previousVersion));
        metadata.addUserMetadata("to_state", String.valueOf(currentVersion));
        metadata.setHeader("Content-Length", deltaFile.length());
        
        uploadFile(deltaFile, objectName, metadata);
    }

    @Override
    public void publishReverseDelta(File reverseDeltaFile, long previousVersion, long currentVersion) {
        String objectName = getS3ObjectName(blobNamespace, "reversedelta", currentVersion);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("from_state", String.valueOf(currentVersion));
        metadata.addUserMetadata("to_state", String.valueOf(previousVersion));
        metadata.setHeader("Content-Length", reverseDeltaFile.length());
        
        uploadFile(reverseDeltaFile, objectName, metadata);
    }
    
    public static String getS3ObjectName(String blobNamespace, String fileType, long lookupVersion) {
        StringBuilder builder = new StringBuilder(getS3ObjectPrefix(blobNamespace, fileType));
        builder.append(Integer.toHexString(HashCodes.hashLong(lookupVersion)));
        builder.append("-");
        builder.append(lookupVersion);
        return builder.toString();
    }
    
    private static String getS3ObjectPrefix(String blobNamespace, String fileType) {
        StringBuilder builder = new StringBuilder(blobNamespace);
        builder.append("/").append(fileType).append("/");
        return builder.toString();
    }
    
    private void uploadFile(File file, String s3ObjectName, ObjectMetadata metadata) {
		try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            Upload upload = s3TransferManager.upload(bucketName, s3ObjectName, is, metadata);
        
            upload.waitForCompletion();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    private synchronized void updateSnapshotIndex(Long newVersion) {
    	/// insert the new version into the list
    	int idx = Collections.binarySearch(snapshotIndex, newVersion);
    	int insertionPoint = Math.abs(idx) - 1;
    	snapshotIndex.add(insertionPoint, newVersion);
    	
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
    	
        ObjectListing listObjects = s3.listObjects(bucketName, getS3ObjectPrefix(blobNamespace, "snapshot"));

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

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
package com.netflix.hollow.example.consumer.infrastructure;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Base16Lower;
import com.netflix.hollow.api.client.HollowBlob;
import com.netflix.hollow.api.client.HollowBlobRetriever;
import com.netflix.hollow.core.memory.encoding.VarInt;
import com.netflix.hollow.example.producer.infrastructure.S3Publisher;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import org.apache.commons.io.IOUtils;

public class S3BlobRetriever implements HollowBlobRetriever {

    private final AmazonS3 s3;
    private final String bucketName;
    private final String blobNamespace;

    public S3BlobRetriever(AWSCredentials credentials, String bucketName, String blobNamespace) {
        this.s3 = new AmazonS3Client(credentials);
        this.bucketName = bucketName;
        this.blobNamespace = blobNamespace;
    }

    @Override
    public HollowBlob retrieveSnapshotBlob(long desiredVersion) {
        try {
            return knownSnapshotBlob(desiredVersion);
        } catch (AmazonS3Exception transitionNotFound) { } 

        /// There was no exact match for a snapshot leading to the desired state. 
        /// We'll use the snapshot index to find the nearest one before the desired state.
        try {
        	File f = downloadFile(S3Publisher.getSnapshotIndexObjectName(blobNamespace));
        	long snapshotIdxLength = f.length();
        	long pos = 0;
        	long currentSnapshotStateId = 0;
        	
        	try(InputStream is = new BufferedInputStream(new FileInputStream(f))) {
        		while(pos < snapshotIdxLength) {
        			long nextGap = VarInt.readVLong(is);
        			
        			if(currentSnapshotStateId + nextGap > desiredVersion) {
        				if(currentSnapshotStateId == 0)
        					return null;
        				
        				return knownSnapshotBlob(currentSnapshotStateId);
        			}
        			
        			currentSnapshotStateId += nextGap;
        			pos += VarInt.sizeOfVLong(nextGap);
        		}
        		
                if(currentSnapshotStateId != 0)
                    return knownSnapshotBlob(currentSnapshotStateId);
        	}
        } catch(IOException e) {
        	throw new RuntimeException(e);
        }
        
        return null;
    }

    @Override
    public HollowBlob retrieveDeltaBlob(long currentVersion) {
        try {
            return knownDeltaBlob("delta", currentVersion);
        } catch (AmazonS3Exception transitionNotFound) {
            return null;
        }
    }

    @Override
    public HollowBlob retrieveReverseDeltaBlob(long currentVersion) {
        try {
            return knownDeltaBlob("reversedelta", currentVersion);
        } catch (AmazonS3Exception transitionNotFound) {
            return null;
        }
    }

    private HollowBlob knownSnapshotBlob(long desiredVersion) {
        String objectName = S3Publisher.getS3ObjectName(blobNamespace, "snapshot", desiredVersion);
        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, objectName);
        long toState = Long.parseLong(objectMetadata.getUserMetaDataOf("to_state"));

        return new S3Blob(objectName, toState);
    }
    
    private HollowBlob knownDeltaBlob(String fileType, long fromVersion) {
        String objectName = S3Publisher.getS3ObjectName(blobNamespace, fileType, fromVersion);
        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, objectName);
        long fromState = Long.parseLong(objectMetadata.getUserMetaDataOf("from_state"));
        long toState = Long.parseLong(objectMetadata.getUserMetaDataOf("to_state"));

        return new S3Blob(objectName, fromState, toState);
    }

    private class S3Blob extends HollowBlob {

        private final String objectName;

        public S3Blob(String objectName, long toVersion) {
            super(toVersion);
            this.objectName = objectName;
        }

        public S3Blob(String objectName, long fromVersion, long toVersion) {
            super(fromVersion, toVersion);
            this.objectName = objectName;
        }

        @Override
        public InputStream getInputStream() throws IOException {

        	final File tempFile = downloadFile(objectName);

            return new BufferedInputStream(new FileInputStream(tempFile)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    tempFile.delete();
                }
            };

        }

    }
    
    private File downloadFile(String objectName) throws IOException {
    	for(int retryCount = 0; retryCount < 3; retryCount++) {
    		try {
		    	File tempFile = new File(System.getProperty("java.io.tmpdir"), objectName.replace('/', '-'));
		    	
		    	S3Object s3Object = s3.getObject(bucketName, objectName);
		    	
		    	MessageDigest md = MessageDigest.getInstance("MD5");
		    	try (
		    			InputStream is = new DigestInputStream(s3Object.getObjectContent(), md);
		    			OutputStream os = new FileOutputStream(tempFile)
		    			) {
		    		IOUtils.copy(is, os);
		    	}
		    	
		    	String expectedMD5 = s3Object.getObjectMetadata().getETag();
		    	String actualMD5 = Base16Lower.encodeAsString(md.digest());
		    	
		    	if (!actualMD5.equals(expectedMD5))
		    		throw new IOException("MD5 sum did not match expected!");
		    	
		    	return tempFile;
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
        throw new IOException("Unable to successfully retrieve stream from S3 after 3 retries");
    }

}

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
import com.netflix.hollow.example.producer.Announcer;

public class S3Announcer implements Announcer {

    public static final String ANNOUNCEMENT_OBJECTNAME = "announced.version";

    private final AmazonS3 s3;
    private final String bucketName;
    private final String blobNamespace;

    public S3Announcer(AWSCredentials credentials, String bucketName, String blobNamespace) {
        this.s3 = new AmazonS3Client(credentials);
        this.bucketName = bucketName;
        this.blobNamespace = blobNamespace;
    }

    @Override
    public void announce(long stateVersion) {
        s3.putObject(bucketName, blobNamespace + "/" + ANNOUNCEMENT_OBJECTNAME, String.valueOf(stateVersion));
    }
    
    
}

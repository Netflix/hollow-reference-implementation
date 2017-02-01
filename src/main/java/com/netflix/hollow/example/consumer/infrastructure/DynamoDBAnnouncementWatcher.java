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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.netflix.hollow.api.client.HollowAnnouncementWatcher;

public class DynamoDBAnnouncementWatcher extends HollowAnnouncementWatcher {

    private final DynamoDB dynamoDB;
    private final String tableName;
    private final String blobNamespace;

    private long latestVersion;

    public DynamoDBAnnouncementWatcher(AWSCredentials credentials, String tableName, String blobNamespace) {
        this.dynamoDB = new DynamoDB(new AmazonDynamoDBClient(credentials));
        this.tableName = tableName;
        this.blobNamespace = blobNamespace;
        this.latestVersion = readLatestVersion();
    }

    @Override
    public long getLatestVersion() {
        return latestVersion;
    }

    @Override
    public void subscribeToEvents() {
        Thread t = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        long currentVersion = readLatestVersion();
                        if (latestVersion != currentVersion) {
                            latestVersion = currentVersion;
                            triggerAsyncRefresh();
                        }

                        Thread.sleep(1000);
                    } catch (Throwable th) {
                        th.printStackTrace();
                    }
                }
            }
        });

        t.setDaemon(true);
        t.start();
    }

    public long readLatestVersion() {
        Table table = dynamoDB.getTable(tableName);

        Item item = table.getItem("namespace", blobNamespace,
                "version, pin_version", null);

        if (item.isPresent("pin_version") && !item.isNull("pin_version"))
            return item.getLong("pin_version");

        return item.getLong("version");
    }
}

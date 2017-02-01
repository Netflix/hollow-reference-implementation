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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.netflix.hollow.example.producer.Announcer;

public class DynamoDBAnnouncer implements Announcer {

    private final DynamoDB dynamoDB;
    private final String tableName;
    private final String blobNamespace;

    public DynamoDBAnnouncer(AWSCredentials credentials, String tableName, String blobNamespace) {
        this.dynamoDB = new DynamoDB(new AmazonDynamoDBClient(credentials));
        this.tableName = tableName;
        this.blobNamespace = blobNamespace;
    }

    @Override
    public void announce(long stateVersion) {
        Table table = dynamoDB.getTable(tableName);

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey("namespace", blobNamespace)
                .withUpdateExpression("set #version = :ver")
                .withNameMap(new NameMap().with("#version", "version"))
                .withValueMap(new ValueMap().withNumber(":ver", stateVersion));

        table.updateItem(updateItemSpec);
    }

}

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
package com.netflix.hollow.example.producer.datamodel;

import com.netflix.hollow.core.write.objectmapper.HollowHashKey;
import java.util.Set;

public class Movie {

    public int id;
    public String title;
    @HollowHashKey(fields="actorId")
    public Set<Actor> actors;
    
    public Movie() { }
    
    public Movie(int id, String title, Set<Actor> actors) {
        this.id = id;
        this.title = title;
        this.actors = actors;
    }

}

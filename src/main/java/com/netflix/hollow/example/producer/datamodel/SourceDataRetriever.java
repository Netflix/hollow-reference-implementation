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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class SourceDataRetriever {
    
	private final List<Movie> allMovies;
	private final List<Actor> allActors;
	private final Random rand;
	
	private int nextMovieId;
	private int nextActorId;
	
	public SourceDataRetriever() {
		this.allMovies = new ArrayList<Movie>();
		this.allActors = new ArrayList<Actor>();
		this.rand = new Random();
		bootstrapData();
	}
	
    /**
     * Retrieve all movies from the source of truth.
     */
    public List<Movie> retrieveAllMovies() {
    	// change a few movie titles
    	int numMovieNamesToChange = rand.nextInt(5);
    	
    	for(int i=0;i<numMovieNamesToChange;i++) {
    		Movie movie = allMovies.get(rand.nextInt(allMovies.size()));
    		movie.title = generateRandomString();
    	}
    	
    	// maybe change an actor name
    	if(rand.nextInt(5) == 1) {
    		Actor actor = allActors.get(rand.nextInt(allActors.size()));
    		actor.actorName = generateRandomString();
    	}
    	
    	// modify a few movie cast lists
    	int numCastListsToEdit = rand.nextInt(5);
    	for(int i=0;i<numCastListsToEdit;i++) {
    		Movie movie = allMovies.get(rand.nextInt(allMovies.size()));
    		
    		int numActorsToRemove = Math.min(rand.nextInt(4), movie.actors.size());
    		int numActorsToAdd = rand.nextInt(4);
    		
    		for(int j=0;j<numActorsToRemove;j++) {
    			Iterator<Actor> iterator = movie.actors.iterator();
				iterator.next();
				iterator.remove();
    		}
    		
    		for(int j=0;j<numActorsToAdd;j++)
    			movie.actors.add(allActors.get(rand.nextInt(allActors.size())));
    	}
    	
    	/// remove a few movies
    	int numMoviesToRemove = rand.nextInt(3);
    	for(int i=0;i<numMoviesToRemove;i++)
    		allMovies.remove(rand.nextInt(allMovies.size()));
    	
    	/// add a few movies
    	int numMoviesToAdd = rand.nextInt(3);
    	for(int i=0;i<numMoviesToAdd;i++)
    		allMovies.add(generateNewRandomMovie());
    	
    	return allMovies;
    }
    
    private List<Movie> bootstrapData() {
        nextActorId = 1000000;
        nextMovieId = 1000000;
        
        for(int i=1;i<1000;i++)
            allActors.add(generateNewRandomActor());
        
        
        for(int i=0;i<10000;i++)
            allMovies.add(generateNewRandomMovie());
        
        return allMovies;
    }

	private Actor generateNewRandomActor() {
		return new Actor(++nextActorId, generateRandomString());
	}
    
    private Movie generateNewRandomMovie() {
        int numActors = rand.nextInt(25) + 1;
        Set<Actor> actors = new HashSet<Actor>();
        
        for(int j=0;j<numActors;j++) {
            actors.add(allActors.get(rand.nextInt(allActors.size())));
        }
        
        return new Movie(++nextMovieId, generateRandomString(), actors);
    }

    private String generateRandomString() {
        Random rand = new Random();
        
        StringBuilder str = new StringBuilder();
        int nameChars = rand.nextInt(20) + 5;
        
        for(int j=0;j<nameChars;j++) {
            str.append((char)(rand.nextInt(26) + 97));
        }
        return str.toString();
    }
    
    


}

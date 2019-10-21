package how.hollow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.test.HollowWriteStateEngineBuilder;
import com.netflix.hollow.test.consumer.TestAnnouncementWatcher;
import com.netflix.hollow.test.consumer.TestBlobRetriever;
import com.netflix.hollow.test.consumer.TestHollowConsumer;
import how.hollow.consumer.api.generated.ActorPrimaryKeyIndex;
import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.consumer.api.generated.MovieAPIHashIndex;
import how.hollow.consumer.api.generated.MoviePrimaryKeyIndex;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class ExampleTest {

  // we will add the snapshot with a version, and make the announcementWatcher see this version
  private long latestVersion = 1L;
  private TestHollowConsumer consumer;
  private TestAnnouncementWatcher announcer = new TestAnnouncementWatcher();

  @Before
  public void before() {
    announcer.setLatestVersion(latestVersion);
    consumer = new TestHollowConsumer.Builder()
        .withAnnouncementWatcher(announcer)
        .withBlobRetriever(new TestBlobRetriever())
        .withGeneratedAPIClass(MovieAPI.class)
        .build();
  }

  @Test
  public void movieIdLookup() throws IOException {
    Movie movie = new Movie(41, "title", Collections.emptySet());

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add("some irrelevant data")
        .add(movie)
        .build());

    MoviePrimaryKeyIndex index = new MoviePrimaryKeyIndex(consumer);
    assertEquals(movie.title, index.findMatch(movie.id).getTitle().getValue());
  }

  @Test
  public void actorIdLookup() throws IOException {
    Actor actor = new Actor(2, "actor name");
    Movie movie = new Movie(1, "title", Set.of(actor));

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add(movie)
        .add(actor)
        .build());

    ActorPrimaryKeyIndex index = new ActorPrimaryKeyIndex(consumer);
    assertEquals(actor.actorName, index.findMatch(actor.actorId).getActorName().getValue());
  }

  @Test
  public void actorIdLookupTransitively() throws IOException {
    Actor actor = new Actor(2, "actor name");
    Movie movie = new Movie(1, "title", Set.of(actor));

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add(movie)
        .build());

    ActorPrimaryKeyIndex index = new ActorPrimaryKeyIndex(consumer);
    assertEquals(actor.actorName, index.findMatch(actor.actorId).getActorName().getValue());
  }

  @Test
  public void actorHashIndexLookup() throws IOException {
    Actor actor = new Actor(2, "actor name");
    Movie movie = new Movie(1, "title", Set.of(actor));

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add(movie)
        .build());

    MovieAPIHashIndex index = new MovieAPIHashIndex(consumer, "Movie", "",
        "actors.element.actorName.value");
    Iterator<how.hollow.consumer.api.generated.Movie> result = index.findMovieMatches("actor name")
        .iterator();
    assertTrue(result.hasNext());
    assertEquals(movie.title, result.next().getTitle().getValue());
    assertFalse(result.hasNext());
    assertFalse(index.findMovieMatches("other name").iterator().hasNext());
  }

  @Test

  public void actorHashIndexLookupReturnsEmptyIterator() throws IOException {
    Movie movie = new Movie(1, "title", Set.of());

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add(movie)
        .build());

    MovieAPIHashIndex index = new MovieAPIHashIndex(consumer, "Movie", "",
        "actors.element.actorName.value");

    Iterator<how.hollow.consumer.api.generated.Movie> result = index.findMovieMatches("other name")
        .iterator();
    assertFalse(result.hasNext());
  }


  @Test
  public void movieIndexUpdate() throws Exception {

    Movie movie = new Movie(1, "title", Set.of());
    Movie newMovie = new Movie(2, "coming soon", Set.of());

    announceNewVersion(latestVersion, new HollowWriteStateEngineBuilder()
        .add(movie)
        .build());

    MoviePrimaryKeyIndex index = new MoviePrimaryKeyIndex(consumer);
    assertNull(index.findMatch(newMovie.id));

    announceNewVersion(2L, new HollowWriteStateEngineBuilder()
        .add(movie)
        .add(newMovie)
        .build());

    assertNotNull(index.findMatch(newMovie.id));
    assertEquals(newMovie.title, index.findMatch(newMovie.id).getTitle().getValue());
  }

  private void announceNewVersion(long version, HollowWriteStateEngine state) throws IOException {
    consumer.addSnapshot(version, state);
    announcer.setLatestVersion(version);
    consumer.triggerRefresh();
  }
}

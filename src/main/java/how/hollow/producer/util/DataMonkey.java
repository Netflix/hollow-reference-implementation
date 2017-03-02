package how.hollow.producer.util;

import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import com.netflix.hollow.api.producer.HollowProducer.WriteState;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;

import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;

public class DataMonkey {
    private static volatile boolean ookook = false;

    private final long initialSeed;
    private final Random germ;
    private final List<Long> seeds;
    private final ActorMonkey bobo;
    private final MovieMonkey bubbles;
    private long offset;


    public DataMonkey() {
        this(new Random().nextLong());
    }

    public DataMonkey(String initialSeed) {
        this(Long.parseUnsignedLong(initialSeed, 16));
    }

    public DataMonkey(long initialSeed) {
        this.initialSeed = initialSeed;
        this.germ = new Random(initialSeed);
        seeds = new ArrayList<>();
        bobo = new ActorMonkey();
        bubbles = new MovieMonkey();
    }

    public WriteState introduceChaos(WriteState writeState) {
        if(!ookook) {
            out.format("I AM THE DATA MONKEY\n  SIMULATING SOURCE DATA CHANGES OVER TIME WITH RANDOM SEED %016x\n  OOK OOK!\n", this.initialSeed);
            ookook = true;
        }
        offset = 0;
        seeds.add(germ.nextLong());
        return new ChaoticWriteState(writeState, this);
    }

    public Object ook(Object o) {
        if(o instanceof Actor) return ook((Actor)o);
        if(o instanceof Movie) return ook((Movie) o);
        return o;
    }

    private Actor ook(final Actor alpha) {
        Actor omega = alpha;
        ++offset;
        Random rand = new Random();
        for(Long seed: seeds) {
            rand.setSeed(seed + offset);
            omega = bobo.ook(rand, omega, alpha);
        }
        return omega;
    }

    private Movie ook(final Movie alpha) {
        Movie omega = alpha;
        if(alpha.actors != null && !alpha.actors.isEmpty()) {
            Set<Actor> s = new LinkedHashSet<>();
            for(Actor actor : omega.actors) s.add(ook(actor));
            omega = new Movie(omega.id, omega.title, omega.releaseYear, s);
        }

        ++offset;
        Random rand = new Random();
        for(Long seed: seeds) {
            rand.setSeed(seed + offset);
            omega = bubbles.ook(rand, omega, alpha);
        }
        return omega;
    }

    public static interface Monkey<T> {
        T ook(Random rand, T t, T alpha);
    }

    private static final class ChaoticWriteState implements WriteState {
        private final HollowObjectMapper chaoticObjectMapper;
        private final long version;

        ChaoticWriteState(WriteState writeState, DataMonkey monkey) {
            version = writeState.getVersion();
            chaoticObjectMapper = new ChaoticObjectMapper(writeState.getStateEngine(), monkey);
        }

        @Override
        public int add(Object o) {
            return chaoticObjectMapper.add(o);
        }

        @Override
        public HollowObjectMapper getObjectMapper() {
            return chaoticObjectMapper;
        }

        @Override
        public HollowWriteStateEngine getStateEngine() {
            return chaoticObjectMapper.getStateEngine();
        }

        @Override
        public long getVersion() {
            return version;
        }
    }

    private static final class ChaoticObjectMapper extends HollowObjectMapper {
        private final DataMonkey monkey;

        ChaoticObjectMapper(HollowWriteStateEngine stateEngine, DataMonkey monkey) {
            super(stateEngine);
            this.monkey = monkey;
        }

        @Override
        public int add(Object o) {
            return super.add(monkey.ook(o));
        }
    }


    private static final class MovieMonkey extends Chaos implements Monkey<Movie> {
        @Override
        public Movie ook(Random rand, Movie m, Movie alpha) {
            Movie omega = m;
            if(chaos(rand, 7177)) {
                omega = new Movie(omega.id, omega.title, omega.releaseYear, shrink(rand, omega.actors, alpha.actors));
            }
            if(chaos(rand, 1103)) {
                omega = new Movie(omega.id, perturbWords(rand, omega.title, "Manos: The Hands of Fate"), omega.releaseYear, omega.actors);
            }
            return omega;
        }
    }

    private static final class ActorMonkey extends Chaos implements Monkey<Actor> {
        @Override
        public Actor ook(Random rand, Actor a, Actor alpha) {
            /// rarely, reset to the original
            if(chaos(rand, 36473)) return alpha;

            Actor omega = a;

            /// commonly shuffle the actor name, otherwise infrequently restore the name
            if(chaos(rand, 1453) && Objects.equals(a.actorName, alpha.actorName)) {
                omega = new Actor(omega.actorId, perturbWords(rand, alpha.actorName, "Athena Fully"));
            } else if (chaos(rand, 13907)) {
                omega = new Actor(omega.actorId, alpha.actorName);
            }

            /// rarely, change the actor ID
            if(chaos(rand, 49757) && omega.actorId > 10) {
                int actorId = intValue(rand, omega.actorId, 20);
                omega = new Actor(actorId, omega.actorName);
            }

            return omega;
        }
    }

    private static abstract class Chaos {
        boolean chaos(Random rand, int chance) {
            return rand.nextInt(chance) == 1;
        }

        <E> Set<E> shrink(Random rand, Set<E> s, Set<E> original) {
            if(s == null || s.isEmpty()) return original;
            List<E> l = new ArrayList<>(s);
            s.remove(l.get(rand.nextInt(s.size())));
            return s;
        }

        String perturbWords(Random rand, String s, String def) {
            if(s == null) return def;
            if(def.equals(s)) return null;

            String[] words = s.split("\\s+");
            List<String> wordList = Arrays.asList(words);
            switch(wordList.size()) {
            case 0: return def;
            case 1: return new StringBuilder(s).reverse().toString();
            default:
                Collections.shuffle(wordList, rand);
                Iterator<String> it = wordList.iterator();
                StringBuilder sb = new StringBuilder(it.next());
                while(it.hasNext()) {
                    sb.append(' ');
                    sb.append(it.next());
                }
                return sb.toString();
            }
        }

        int intValue(Random rand, int base, int dev) {
            return base + rand.nextInt(dev) - (dev/2);
        }
    }
}

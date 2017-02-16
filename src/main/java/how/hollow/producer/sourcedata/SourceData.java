package how.hollow.producer.sourcedata;

import static how.hollow.producer.sourcedata.SourceData.ScanState.NEXT_EOL;
import static how.hollow.producer.sourcedata.SourceData.ScanState.NEXT_PIPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newByteChannel;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SourceData implements Iterable<SourceData.Row>, AutoCloseable {

    private final FileChannel data;
    private final Map<Integer,Row> rows;
    private boolean closed;
    private boolean indexed;

    public SourceData(String filename) {
        try {
            Path dataPath = Paths.get(getClass().getResource("/" + filename).toURI());
            data = (FileChannel)newByteChannel(dataPath, READ);
            rows = new LinkedHashMap<>();
        } catch(URISyntaxException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SourceData.Row get(int id) {
        if(!indexed) index();
        return rows.get(id);
    }

    @Override
    public Iterator<SourceData.Row> iterator() {
        if(!indexed) index();
        return rows.values().iterator();
    }

    private void index() {
        try {
            long offset;
            int id = -1;
            ByteBuffer bb = ByteBuffer.allocate(1);
            byte[] idBytes = new byte[32];
            ByteBuffer idBuffer = ByteBuffer.wrap(idBytes);
            ScanState state = NEXT_PIPE;

            offset = data.position(0).position();
            while(data.read(bb) != -1) {
                bb.flip();
                while(bb.hasRemaining()) {
                    byte b = bb.get();

                    if(b == state.delim) {
                        long nextOffset = data.position() - bb.remaining();
                        switch(state) {
                        case NEXT_PIPE:
                            idBuffer.flip();
                            String idString = new String(idBytes, UTF_8);
                            id = Integer.parseInt(idString.trim());
                            idBuffer.clear();
                            state = NEXT_EOL;
                            break;
                        case NEXT_EOL:
                            rows.put(id, new Row(data, id, offset, (int)(nextOffset - offset - 1)));
                            id = -1;
                            state = NEXT_PIPE;

                            break;
                        default:
                            throw new IllegalStateException();
                        }
                        offset = nextOffset;
                    } else if (state == NEXT_PIPE) {
                        idBuffer.put(b);
                    }
                }
                bb.clear();
            }
            indexed = true;
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    enum ScanState {
        NEXT_PIPE('|'),
        NEXT_EOL('\n');
        final byte delim;

        ScanState(char delim) {
            this.delim = (byte)delim;
        }
    }

    @Override
    public void close() {
        if(closed) return;
        try {
            data.close();
        } catch(IOException ignored) {
            ignored.printStackTrace();
        } finally {
            rows.clear();
            closed = true;
        }
    }

    public static final class Row implements Iterable<Column> {
        public final int id;

        private final FileChannel data;
        private final long offset;
        private final int length;

        Row(FileChannel data, int id, long offset, int length) {
            this.data = data;
            this.id = id;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public Iterator<Column> iterator() {
            try {
                List<Column> result = new ArrayList<>();
                for(String s : line().split("\\s*\\|\\s*")) {
                    result.add(new Column(s));
                }
                return result.iterator();
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private String line() throws IOException {
            byte[] bytes = new byte[length];
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            data.read(bb, offset);
            String line = new String(bytes, UTF_8);
            return line;
        }

        @Override
        public String toString() {
            return "SourceData.Row[" + id + ", " + offset + "]";
        }
    }

    public static final class Column {
        public final String value;

        Column(String value) {
            this.value = value == null ? null : value.trim();
        }

        public int toInt() {
            int result;
            try {
                result = Integer.parseInt(value);
            } catch(NumberFormatException ex) {
                result = Integer.MIN_VALUE;
            }
            return result;
        }

        public Integer toInteger() {
            Integer result;
            try {
                result = Integer.valueOf(value);
            } catch(NumberFormatException ex) {
                result = null;
            }
            return result;
        }

        public Set<Integer> toIds() {
            Set<Integer> result = Collections.emptySet();
            if(value != null) {
                Set<Integer> set = new LinkedHashSet<>();
                for(String idString: value.split("\\s*,\\s*")) {
                    Integer id = Integer.valueOf(idString.trim());
                    set.add(id);
                }
                result = set;
            }
            return result;
        }
    }
}

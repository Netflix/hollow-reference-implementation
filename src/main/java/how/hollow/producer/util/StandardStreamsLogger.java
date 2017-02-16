package how.hollow.producer.util;

import static java.lang.Math.floor;
import static java.lang.Math.log10;
import static java.lang.Math.max;

import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.netflix.hollow.api.producer.AbstractHollowProducerListener;
import com.netflix.hollow.api.producer.HollowProducerListener;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;

public class StandardStreamsLogger extends AbstractHollowProducerListener implements HollowProducerListener {

    private final PrintStream out;
    private final PrintStream err;

    public StandardStreamsLogger() {
        this(System.out, System.err);
    }

    StandardStreamsLogger(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public void onProducerInit(long elapsed, TimeUnit unit) {
        info("DATA MODEL INITIALIZED IN %d%s\n", elapsed, abbreviate(unit));
    }

    @Override
    public void onProducerRestoreStart(long restoreVersion) {
        info("RESTORING TO %s\n", restoreVersion);
    }

    @Override
    public void onProducerRestoreComplete(RestoreStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("DELTA CHAIN RESUMED AT %s. RESTORE COMPLETED IN %d%s\n", status.getVersionReached(), elapsed, abbreviate(unit));
            break;
        case FAIL:
            error("RESTORE UNSUCCESSFUL; desired=%s reached=%S. COMPLETED IN %d%s\n",
                    status.getDesiredVersion(), status.getVersionReached(), elapsed, abbreviate(unit));
            break;
        }
    }

    @Override
    public void onNewDeltaChain(long version) {
        info("STARTING NEW DELTA CHAIN AT %s\n", version);
    }

    @Override
    public void onCycleStart(long version) {
        info("BEGIN %d\n", version);
    }

    @Override
    public void onNoDeltaAvailable(long version) {
        info("      %d NO SOURCE DATA CHANGES. NOTHING TO DO THIS CYCLE.\n", version);
    }

    @Override
    public void onPublishStart(long version) {
        info("      %d PUBLISH STARTED...\n", version);
    }

    @Override
    public void onPublishComplete(ProducerStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("      %d PUBLISH SUCCEEDED IN %d%s\n", status.getVersion(), elapsed, abbreviate(unit));
            break;
        case FAIL:
            error("PUBLISH", status.getVersion(), status.getCause(), elapsed, unit);
            break;
        }
    }

    @Override
    public void onIntegrityCheckStart(long version) {
        info("      %d INTEGRITY CHECK STARTED...\n", version);
    }

    @Override
    public void onIntegrityCheckComplete(ProducerStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("      %d INTEGRITY CHECK SUCCEEDED IN %d%s\n", status.getVersion(), elapsed, abbreviate(unit));
            Collection<HollowTypeReadState> typeStates = status.getReadState().getStateEngine().getTypeStates();
            int nameWidth = 0;
            int footprintWidth = 0;
            for(HollowTypeReadState ts : typeStates) {
                nameWidth = max(nameWidth, ts.getSchema().getName().length());
                footprintWidth = max(footprintWidth, (int)floor(log10(ts.getApproximateHeapFootprintInBytes()) + 1));
            }
            String format = String.format("                          %%-%ds %%%dd bytes\n", nameWidth, footprintWidth);
            for(HollowTypeReadState ts : typeStates) {
                info(format, ts.getSchema().getName(), ts.getApproximateHeapFootprintInBytes());
            }
            break;
        case FAIL:
            error("INTEGRITY CHECK", status.getVersion(), status.getCause(), elapsed, unit);
            break;
        }
    }

    @Override
    public void onValidationStart(long version) {
        info("      %d VALIDATION STARTED...\n", version);
    }

    @Override
    public void onValidationComplete(ProducerStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("      %d VALIDATION SUCCEEDED IN %d%s\n", status.getVersion(), elapsed, abbreviate(unit));
            break;
        case FAIL:
            error("VALIDATION", status.getVersion(), status.getCause(), elapsed, unit);
            break;
        }
    }

    @Override
    public void onAnnouncementStart(long version) {
        info("      %d ANNOUNCEMENT STARTED...\n", version);
    }

    @Override
    public void onAnnouncementComplete(ProducerStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("      %d ANNOUNCEMENT SUCCEEDED IN %d%s\n", status.getVersion(), elapsed, abbreviate(unit));
            break;
        case FAIL:
            error("ANNOUNCEMENT", status.getVersion(), status.getCause(), elapsed, unit);
            break;
        }
    }

    @Override
    public void onCycleComplete(ProducerStatus status, long elapsed, TimeUnit unit) {
        switch(status.getStatus()) {
        case SUCCESS:
            info("      %d CYCLE SUCCEEDED IN %d%s\n", status.getVersion(), elapsed, abbreviate(unit));
            break;
        case FAIL:
            error("CYCLE", status.getVersion(), status.getCause(), elapsed, unit);
            break;
        }
    }

    private void error(String stage, long version, Throwable cause, long elapsed, TimeUnit unit) {
        String versionString = version != Long.MIN_VALUE ? String.valueOf(version) : "                 ";
        String causeString = cause != null ? cause.getMessage() : "unknown cause";
        error("ERROR %s %s FAILED in %d%s. CAUSE: %s\n", versionString, stage, elapsed, abbreviate(unit), causeString);
    }

    private String abbreviate(TimeUnit unit) {
        switch(unit) {
        case DAYS: return "days";
        case HOURS: return "hrs";
        case MICROSECONDS: return "mu";
        case MILLISECONDS: return "ms";
        case MINUTES: return "min";
        case NANOSECONDS: return "ns";
        case SECONDS: return "s";
        default: return "balloons";
        }
    }

    protected void info(String s) {
        out.println(s);
    }

    protected void info(String format, Object...args) {
        out.format(format, args);
    }

    protected void error(String s) {
        err.println(s);
    }

    protected void error(String format, Object...args) {
        err.format(format, args);
    }
}

package de.zalando.pgobserver.gatherer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

public class BgwriterStatsGatherer extends ADBGatherer {

    private static final Logger LOG = LoggerFactory.getLogger(BgwriterStatsGatherer.class);

    private List<BgwriterValue> valueStore = new ArrayList<BgwriterValue>();

    public BgwriterStatsGatherer(final String gathererName, final Host h, final ScheduledThreadPoolExecutor executor,
            final long intervalInSeconds) {
        super(gathererName, h, executor, intervalInSeconds);

    }

    @Override
    protected boolean gatherData() {

        try {
            valueStore.add(gatherService.getBgwriterStats(host.id));

            LOG.info("finished getting background writer data " + host.getName());

            while (!valueStore.isEmpty()) {
                BgwriterValue toStore = valueStore.get(0);
                writerService.saveBgwriterStats(toStore);
                valueStore.remove(0);

            }
        } catch (Exception e) {

            LOG.error(this.toString(), e);
            return false;
        }

        return true;
    }

}

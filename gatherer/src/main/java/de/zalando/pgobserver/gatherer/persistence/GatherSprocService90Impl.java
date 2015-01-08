package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.AbstractSProcService;
import de.zalando.sprocwrapper.dsprovider.DataSourceProvider;

public class GatherSprocService90Impl extends AbstractSProcService<GatherSprocService90, DataSourceProvider>
    implements GatherSprocService90 {

    public GatherSprocService90Impl(final DataSourceProvider ps) {
        super(ps, GatherSprocService90.class);
    }

    @Override
    public BgwriterValue getBgwriterStats(final int hostId) {

        return sproc.getBgwriterStats(hostId);
    }
}

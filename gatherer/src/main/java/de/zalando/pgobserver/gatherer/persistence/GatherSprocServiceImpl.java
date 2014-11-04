package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.AbstractSProcService;
import de.zalando.sprocwrapper.dsprovider.DataSourceProvider;

public class GatherSprocServiceImpl extends AbstractSProcService<GatherSprocService, DataSourceProvider>
    implements GatherSprocService {

    public GatherSprocServiceImpl(final DataSourceProvider ps) {
        super(ps, GatherSprocService.class);
    }

    @Override
    public BgwriterValue getBgwriterStats(final int hostId) {

        return sproc.getBgwriterStats(hostId);
    }
}

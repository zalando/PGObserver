package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.AbstractSProcService;
import de.zalando.sprocwrapper.SProcParam;
import de.zalando.sprocwrapper.dsprovider.DataSourceProvider;

public class PgoWriterSprocServiceImpl extends AbstractSProcService<PgoWriterSprocService, DataSourceProvider>
    implements PgoWriterSprocService {

    public PgoWriterSprocServiceImpl(final DataSourceProvider datasource) {
        super(datasource, PgoWriterSprocService.class);
    }

    @Override
    public void saveBgwriterStats(@SProcParam final BgwriterValue bgwriter) {
        sproc.saveBgwriterStats(bgwriter);
    }

}

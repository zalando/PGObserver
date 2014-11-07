package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.SProcCall;
import de.zalando.sprocwrapper.SProcParam;
import de.zalando.sprocwrapper.SProcService;

/**
 * This interface is used for writing the gathered data to the pgobserver database.
 *
 * @author  slitsche
 */

@SProcService
public interface PgoWriterSprocService {

    @SProcCall
    void saveBgwriterStats(@SProcParam BgwriterValue bgwriter);
}

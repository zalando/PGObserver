package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.SProcCall;
import de.zalando.sprocwrapper.SProcParam;
import de.zalando.sprocwrapper.SProcService;

@SProcService
public interface GatherSprocService {

    @SProcCall(
        sql = "select ? as host_id, current_timestamp as log_timestamp, checkpoints_timed, checkpoints_req, checkpoint_write_time, "
                + "       checkpoint_sync_time, buffers_checkpoint, buffers_clean, maxwritten_clean, "
                + "       buffers_backend, buffers_backend_fsync, buffers_alloc, stats_reset "
                + "  from pg_stat_bgwriter;"

    )
    BgwriterValue getBgwriterStats(@SProcParam int hostId);

}

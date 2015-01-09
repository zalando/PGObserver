package de.zalando.pgobserver.gatherer.persistence;

import de.zalando.pgobserver.gatherer.domain.BgwriterValue;

import de.zalando.sprocwrapper.SProcCall;
import de.zalando.sprocwrapper.SProcParam;
import de.zalando.sprocwrapper.SProcService;

@SProcService
public interface GatherSprocService90 extends GatherSprocService{


    @SProcCall(
        sql = "select ? as host_id, current_timestamp as log_timestamp, checkpoints_timed, checkpoints_req, 0 as checkpoint_write_time, "
                + "       0 as checkpoint_sync_time, buffers_checkpoint, buffers_clean, maxwritten_clean, "
                + "       buffers_backend, 0 as buffers_backend_fsync, buffers_alloc, '01-01-1970'::timestamp stats_reset "
                + "  from pg_stat_bgwriter;"
    )
    BgwriterValue getBgwriterStats(@SProcParam int hostId);

}

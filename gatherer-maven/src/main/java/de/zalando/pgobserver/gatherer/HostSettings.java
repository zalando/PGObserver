package de.zalando.pgobserver.gatherer;

/**
 * @author  jmussler Configuration values: check intervals in minutes 0 if disabled
 */
public class HostSettings {
    private int loadGatherInterval = 0;
    private int tableIoGatherInterval = 0;
    private int sprocGatherInterval = 0;

    public void setUiLongName(final String uiLongName) {
        this.uiLongName = uiLongName;
    }

    public void setUiShortName(final String uiShortName) {
        this.uiShortName = uiShortName;
    }

    private int tableStatsGatherInterval = 0;

    public void setLoadGatherInterval(final int loadGatherInterval) {
        this.loadGatherInterval = loadGatherInterval;
    }

    public void setSprocGatherInterval(final int sprochGatherInterval) {
        this.sprocGatherInterval = sprochGatherInterval;
    }

    public void setTableIoGatherInterval(final int tableIoGatherInterval) {
        this.tableIoGatherInterval = tableIoGatherInterval;
    }

    public void setTableStatsGatherInterval(final int tableStatsGatherInterval) {
        this.tableStatsGatherInterval = tableStatsGatherInterval;
    }

    private String uiLongName = "";
    private String uiShortName = "";
    
    public String getUiShortName() {
        return uiShortName;
    }
    
    public String getUiLongName() {
        return uiLongName;
    }

    public int getLoadGatherInterval() {
        return loadGatherInterval * 60;
    }

    public int getSprocGatherInterval() {
        return sprocGatherInterval * 60;
    }

    public int getTableIoStatsGatherInterval() {
        return tableIoGatherInterval * 60;
    }

    public int getTableStatsGatherInterval() {
        return tableStatsGatherInterval * 60;
    }

    public boolean isLoadGatherEnabled() {
        return loadGatherInterval > 0;
    }

    public boolean isTableIoStatsGatherEnabled() {
        return tableIoGatherInterval > 0;
    }

    public boolean isSprocGatherEnabled() {
        return sprocGatherInterval > 0;
    }

    public boolean isTableStatsGatherEnabled() {
        return tableStatsGatherInterval > 0;
    }

}

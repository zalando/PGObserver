package de.zalando.pgobserver.gatherer;

public class HostSettings {
    private int loadGatherInterval = 0;
    private int tableIoGatherInterval = 0;
    private int indexStatsGatherInterval = 0;
    private int sprocGatherInterval = 0;
    private int schemaStatsGatherInterval = 0;
    private int blockingStatsGatherInterval = 0;
    private int useTableSizeApproximation = 0;
    private int tableStatsGatherInterval = 0;
    private int statStatementsGatherInterval = 0;
    private int statDatabaseGatherInterval = 0;
    private int statBgwriterGatherInterval = 0;

    public int getStatStatementsGatherInterval() {
        return statStatementsGatherInterval * 60;
    }

    public void setStatStatementsGatherInterval(final int statStatementsGathererInterval) {
        this.statStatementsGatherInterval = statStatementsGathererInterval;
    }

    public int getStatDatabaseGatherInterval() {
        return statDatabaseGatherInterval * 60;
    }

    public void setStatDatabaseGatherInterval(final int statDatabaseGatherInterval) {
        this.statDatabaseGatherInterval = statDatabaseGatherInterval;
    }

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

    public void setIndexStatsGatherInterval(final int indexStatsGatherInterval) {
        this.indexStatsGatherInterval = indexStatsGatherInterval;
    }

    public void setSchemaStatsGatherInterval(final int schemaStatsGatherInterval) {
        this.schemaStatsGatherInterval = schemaStatsGatherInterval;
    }

    public void setBlockingStatsGatherInterval(final int blockingStatsGatherInterval) {
        this.blockingStatsGatherInterval = blockingStatsGatherInterval;
    }

    public void setUseTableSizeApproximation(final int useTableSizeApproximation) {
        this.useTableSizeApproximation = useTableSizeApproximation;
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

    public int getIndexStatsGatherInterval() {
        return indexStatsGatherInterval * 60;
    }

    public int getSchemaStatsGatherInterval() {
        return schemaStatsGatherInterval * 60;
    }

    public int getBlockingStatsGatherInterval() {
        return blockingStatsGatherInterval * 60;
    }

    public int getUseTableSizeApproximation() {
        return useTableSizeApproximation;
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

    public boolean isIndexStatsGatherEnabled() {
        return indexStatsGatherInterval > 0;
    }

    public boolean isSchemaStatsGatherEnabled() {
        return schemaStatsGatherInterval > 0;
    }

    public boolean isBlockingStatsGatherEnabled() {
        return blockingStatsGatherInterval > 0;
    }

    public boolean isStatStatementsGatherEnabled() {
        return statStatementsGatherInterval > 0;
    }

    public boolean isStatDatabaseGatherEnabled() {
        return statDatabaseGatherInterval > 0;
    }

    public int getStatBgwriterGatherInterval() {
        return statBgwriterGatherInterval * 60;
    }

    public void setStatBgwriterGatherInterval(final int statBgwriterGatherInterval) {
        this.statBgwriterGatherInterval = statBgwriterGatherInterval;
    }

    public boolean isStatBwriterGatherEnabled() {
        return this.statBgwriterGatherInterval > 0;
    }

}

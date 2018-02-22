PGObserver's gatherer
=====================

## Building

Build single jar file with dependencies included:

```
mvn clean verify assembly:single
```

## Running

 * copy `.pgobserver.yaml` to user's home folder
 * configure `.pgobserver.yaml` to access the database where you store gathered metrics
 * execute `./run.sh` script to start

## Configuration

By default `gatherer` searches for configuration file in `$HOME/.pgobserver.yaml` unless `gatherer.jar /path/to/config.yaml` is executed with extra path argument.

### Environment variable

Configuration from `ENV` variables takes precedence over YAML config. Following variables are recognized:

* `PGOBS_HOST`
* `PGOBS_PORT`
* `PGOBS_DATABASE`
* `PGOBS_USER`
* `PGOBS_PASSWORD`
* `HTTP_PORT` by default `gatherer` starts HTTP server on port `8182`


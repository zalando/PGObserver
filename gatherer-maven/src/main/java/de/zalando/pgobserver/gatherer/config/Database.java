package de.zalando.pgobserver.gatherer.config;

/**
 *
 * @author jmussler
 */
public class Database {
    public String name;
    public String host;
    public int port;
    public String frontend_user;
    public String frontend_password;
    public String backend_user;
    public String backend_password;
    public String gather_group = "host1";
}

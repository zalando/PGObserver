RESET role;
SET role TO postgres;

CREATE SCHEMA z_blocking;

GRANT USAGE ON SCHEMA z_blocking to public;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA z_blocking GRANT SELECT ON TABLES TO public;

SET search_path TO z_blocking, public;

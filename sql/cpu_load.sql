CREATE SCHEMA zz_utils;

GRANT USAGE ON SCHEMA zz_utils TO public;

DROP TYPE IF EXISTS zz_utils.load_average CASCADE;

CREATE TYPE zz_utils.load_average AS ( load_1min real, load_5min real, load_15min real );

CREATE OR REPLACE FUNCTION zz_utils.get_load_average() RETURNS zz_utils.load_average AS
$$
"""
  select * from zz_utils.get_load_average();
"""
from os import getloadavg
return getloadavg()
$$ LANGUAGE plpythonu VOLATILE SECURITY DEFINER;

ALTER FUNCTION zz_utils.get_load_average() OWNER TO postgres;

GRANT EXECUTE ON FUNCTION zz_utils.get_load_average() TO public;

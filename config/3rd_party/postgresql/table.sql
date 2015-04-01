-- Table: XXXXXXXX

-- DROP TABLE XXXXXXXX;

CREATE TABLE XXXXXXXX
(
  procid integer,
  severity text,
  facility text,
  app_name text,
  source text,
  programname text,
  syslogtag text,
  uuid text NOT NULL,
  timereported timestamp with time zone,
  timegenerated timestamp with time zone,
  trusted_pid integer,
  trusted_uid integer,
  trusted_gid integer,
  trusted_comm text,
  trusted_exe text,
  trusted_cmdline text,
  message text,
  tags text[] COLLATE pg_catalog."C.UTF-8",
  custom_fields jsonb,
  timehmac timestamp with time zone,
  hmac text,
  structured_data jsonb,
  CONSTRAINT uuid_pk PRIMARY KEY (uuid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE XXXXXXXX
  OWNER TO syslog;

-- Index: appname_idx

-- DROP INDEX appname_idx;

CREATE INDEX appname_idx
  ON XXXXXXXX
  USING btree
  (app_name COLLATE pg_catalog."C.UTF-8" text_pattern_ops);

-- Index: cfields_idx

-- DROP INDEX cfields_idx;

CREATE INDEX cfields_idx
  ON XXXXXXXX
  USING gin
  (custom_fields);

-- Index: facility_idx

-- DROP INDEX facility_idx;

CREATE INDEX facility_idx
  ON XXXXXXXX
  USING btree
  (facility COLLATE pg_catalog."C.UTF-8" text_pattern_ops);

-- Index: generated_idx

-- DROP INDEX generated_idx;

CREATE INDEX generated_idx
  ON XXXXXXXX
  USING btree
  (timegenerated);

-- Index: message_idx

-- DROP INDEX message_idx;

CREATE INDEX message_idx
  ON XXXXXXXX
  USING btree
  (message COLLATE pg_catalog."C.UTF-8" text_pattern_ops);

-- Index: reported_idx

-- DROP INDEX reported_idx;

CREATE INDEX reported_idx
  ON XXXXXXXX
  USING btree
  (timereported);

-- Index: severity_idx

-- DROP INDEX severity_idx;

CREATE INDEX severity_idx
  ON XXXXXXXX
  USING btree
  (severity COLLATE pg_catalog."C.UTF-8" text_pattern_ops);

-- Index: source_idx

-- DROP INDEX source_idx;

CREATE INDEX source_idx
  ON XXXXXXXX
  USING btree
  (source COLLATE pg_catalog."C.UTF-8" text_pattern_ops);

-- Index: structured_data_idx

-- DROP INDEX structured_data_idx;

CREATE INDEX structured_data_idx
  ON XXXXXXXX
  USING gin
  (structured_data);

-- Index: tags_idx

-- DROP INDEX tags_idx;

CREATE INDEX tags_idx
  ON XXXXXXXX
  USING gin
  (tags COLLATE pg_catalog."C.UTF-8");


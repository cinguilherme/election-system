(ns election-system.db.schema
  "Database schema definitions and Track 08 temporal storage-schema configuration
   for SQLite, PostgreSQL, and MySQL."
  (:require [d-core.core.databases.protocols.simple-sql :as sql]))

(def storage-schema
  {:tables
   {:ballots
    {:temporal {:cast_at {:kind :instant :timezone :utc}}}
    :precinct_tallies
    {:temporal {:updated_at {:kind :instant :timezone :utc}}}
    :election_tallies
    {:temporal {:updated_at {:kind :instant :timezone :utc}}}}})

(defn- ddl-for-engine
  [engine]
  (case (or engine :sqlite)
    :postgres
    ["CREATE TABLE IF NOT EXISTS ballots (
        ballot_id VARCHAR(128) PRIMARY KEY,
        election_id VARCHAR(128) NOT NULL,
        precinct_id VARCHAR(128) NOT NULL,
        voter_id VARCHAR(128) NOT NULL,
        candidate_id VARCHAR(128) NOT NULL,
        cast_at TIMESTAMPTZ NOT NULL,
        UNIQUE (election_id, precinct_id, voter_id)
      )"
     "CREATE TABLE IF NOT EXISTS precinct_tallies (
        election_id VARCHAR(128) NOT NULL,
        precinct_id VARCHAR(128) NOT NULL,
        tallies_json TEXT NOT NULL,
        total_votes BIGINT NOT NULL,
        flushes BIGINT NOT NULL,
        last_sequence VARCHAR(128),
        updated_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (election_id, precinct_id)
      )"
     "CREATE TABLE IF NOT EXISTS election_tallies (
        election_id VARCHAR(128) PRIMARY KEY,
        tallies_json TEXT NOT NULL,
        precincts_reporting BIGINT NOT NULL,
        total_votes BIGINT NOT NULL,
        flushes BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      )"
     "CREATE TABLE IF NOT EXISTS actor_snapshots (
        entity_type VARCHAR(128) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        state_payload TEXT NOT NULL,
        sequence_num VARCHAR(128),
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (entity_type, entity_id)
      )"]

    :mysql
    ["CREATE TABLE IF NOT EXISTS ballots (
        ballot_id VARCHAR(128) PRIMARY KEY,
        election_id VARCHAR(128) NOT NULL,
        precinct_id VARCHAR(128) NOT NULL,
        voter_id VARCHAR(128) NOT NULL,
        candidate_id VARCHAR(128) NOT NULL,
        cast_at DATETIME(6) NOT NULL,
        UNIQUE KEY uq_voter (election_id, precinct_id, voter_id)
      )"
     "CREATE TABLE IF NOT EXISTS precinct_tallies (
        election_id VARCHAR(128) NOT NULL,
        precinct_id VARCHAR(128) NOT NULL,
        tallies_json TEXT NOT NULL,
        total_votes BIGINT NOT NULL,
        flushes BIGINT NOT NULL,
        last_sequence VARCHAR(128),
        updated_at DATETIME(6) NOT NULL,
        PRIMARY KEY (election_id, precinct_id)
      )"
     "CREATE TABLE IF NOT EXISTS election_tallies (
        election_id VARCHAR(128) PRIMARY KEY,
        tallies_json TEXT NOT NULL,
        precincts_reporting BIGINT NOT NULL,
        total_votes BIGINT NOT NULL,
        flushes BIGINT NOT NULL,
        updated_at DATETIME(6) NOT NULL
      )"
     "CREATE TABLE IF NOT EXISTS actor_snapshots (
        entity_type VARCHAR(128) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        state_payload LONGTEXT NOT NULL,
        sequence_num VARCHAR(128),
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (entity_type, entity_id)
      )"]

    ;; Default :sqlite
    ["CREATE TABLE IF NOT EXISTS ballots (
        ballot_id TEXT PRIMARY KEY,
        election_id TEXT NOT NULL,
        precinct_id TEXT NOT NULL,
        voter_id TEXT NOT NULL,
        candidate_id TEXT NOT NULL,
        cast_at TEXT NOT NULL,
        UNIQUE (election_id, precinct_id, voter_id)
      )"
     "CREATE TABLE IF NOT EXISTS precinct_tallies (
        election_id TEXT NOT NULL,
        precinct_id TEXT NOT NULL,
        tallies_json TEXT NOT NULL,
        total_votes INTEGER NOT NULL,
        flushes INTEGER NOT NULL,
        last_sequence TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (election_id, precinct_id)
      )"
     "CREATE TABLE IF NOT EXISTS election_tallies (
        election_id TEXT PRIMARY KEY,
        tallies_json TEXT NOT NULL,
        precincts_reporting INTEGER NOT NULL,
        total_votes INTEGER NOT NULL,
        flushes INTEGER NOT NULL,
        updated_at TEXT NOT NULL
      )"
     "CREATE TABLE IF NOT EXISTS actor_snapshots (
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        state_payload TEXT NOT NULL,
        sequence_num TEXT,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (entity_type, entity_id)
      )"]))

(defn create-tables!
  "Executes DDL to initialize election tables on `db` for `engine` (`:sqlite`, `:postgres`, `:mysql`)."
  ([db]
   (create-tables! db :sqlite))
  ([db engine]
   (doseq [stmt (ddl-for-engine engine)]
     (sql/execute! db [stmt] {}))
   true))

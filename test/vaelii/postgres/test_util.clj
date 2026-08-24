;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.postgres.test-util
  "The server gate, shared by every test in this repo.

  **The suite must not need a server.**  Every db-touching test is gated on a reachable
  Postgres and **skips with a printed reason** when there is none, so `lein test` on a
  machine with no database is green and means what it says.  The gate is never inverted
  — live unless something disables it — because a stray `PGHOST` in an environment would
  then silently change what a run covers.

  Point the suite at a server with `VAELII_PG_URL` or the `POSTGRES_*` variables; it
  defaults to `localhost:5432/vaelii` as the current OS user.  `docker compose -f
  docker-compose.test.yml up -d` in this repo's root stands one up.

  Each test works inside a **schema of its own**, created on entry and dropped on the way
  out, so a run leaves the database as it found it and two suites can share one server."
  (:require [clojure.test :refer [is]]
            [next.jdbc :as jdbc]))

(defn db-spec
  "The db-spec the suite connects with: `VAELII_PG_URL` if set, else the `POSTGRES_*`
  variables over `localhost:5432/vaelii` as the current OS user."
  []
  (if-let [url (System/getenv "VAELII_PG_URL")]
    {:jdbcUrl url}
    {:dbtype   "postgresql"
     :host     (or (System/getenv "POSTGRES_HOST") "localhost")
     :port     (or (some-> (System/getenv "POSTGRES_PORT") parse-long) 5432)
     :dbname   (or (System/getenv "POSTGRES_DB") "vaelii")
     :user     (or (System/getenv "POSTGRES_USER") (System/getProperty "user.name"))
     :password (System/getenv "POSTGRES_PASSWORD")}))

(def live
  "The spec if a server answers, else nil — probed once, with a printed reason."
  (delay
    (let [spec (db-spec)]
      (try
        (with-open [c (jdbc/get-connection spec)]
          (jdbc/execute-one! c ["SELECT 1"]))
        spec
        (catch Exception e
          (println "vaelii.postgres: no reachable Postgres (" (ex-message e)
                   ") — skipping the db tests")
          nil)))))

(defn served
  "Call `(f ds)` with the live db-spec, or skip — passing, and printed — when no server
  answers.  A function, not a macro, so the binding stays plain."
  [f]
  (if-let [ds @live]
    (f ds)
    (is true "skipped: no Postgres")))

(defn with-schema
  "Call `(f spec)` with `ds` plus a `:schema` of its own, dropping the schema and
  everything in it afterwards — so a failed test leaves no tables behind for the next
  one to read."
  [ds schema f]
  (jdbc/execute-one! ds [(str "DROP SCHEMA IF EXISTS " schema " CASCADE")])
  (try
    (f (assoc ds :schema schema))
    (finally
      (jdbc/execute-one! ds [(str "DROP SCHEMA IF EXISTS " schema " CASCADE")]))))

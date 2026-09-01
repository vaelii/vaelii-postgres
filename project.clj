(defproject com.vaelii/postgres "0.15.0"
  :description "Postgres targets for vaelii's storage seams, in two independent
                lanes. The record store (vaelii.postgres.record-store) puts a KB's
                durable ground truth in a database — core selects it as :pg-memory
                or :pg-disk-log — with COPY on the bulk path, a fetch LRU in front of a
                round-trip point read, and cursor-streamed enumerations. The
                snapshot sink (vaelii.postgres.snapshot) is a SnapshotSink /
                SnapshotSource, so a KB image — the index projection today, any of
                io.snapshot's named sections — lands in a database as a few bulk
                transactions and reads back the same way. An Apache-2.0 adapter on
                the SSPL engine, depending on core, never depended on by it."
  :license {:name "Apache-2.0" :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :url "https://github.com/vaelii/vaelii-postgres"
  :scm {:name "git" :url "https://github.com/vaelii/vaelii-postgres"}
  ;; The POM's homepage and source link. Missing on the first cut, which is how
  ;; `lein deploy` came to warn about `:url` with the release already promoted —
  ;; and a Clojars coordinate keeps whatever POM it was published with.
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org/"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases false}]]
  :source-paths ["src"]
  :test-paths   ["test"]

  ;; Reflection is a bug on the JDBC paths — surface it at compile time. The
  ;; :test profile flips it off so the test tree doesn't spam `lein test`.
  :global-vars {*warn-on-reflection* true}

  :dependencies
  [[org.clojure/clojure "1.12.5"]
   ;; the engine.  checkouts/vaelii -> ../vaelii shadows this with the dev-core
   ;; SOURCE, so a dev run reads whatever that tree is; the coordinate below is what a
   ;; CONSUMER of this adapter resolves, and it is a floor rather than a convenience.
   ;; The record store implements core's Prefetching seam, which lands in 0.11.1 — so
   ;; that is the floor, and it stays a SNAPSHOT only until 0.11.1 is cut.
   [com.vaelii/vaelii "0.15.0"]
   ;; the sink's own deps — declared here, not leaned on through core, so a change
   ;; in core's deps cannot break this adapter's load
   [com.github.seancorfield/next.jdbc "1.3.1118"]
   [org.postgresql/postgresql "42.7.11"]
   [com.taoensso/nippy "3.8.1"]
   ;; the record store's pool.  A live records backend is one writer and N readers by
   ;; the engine's single-writer contract, and a db-spec without a pool opens a
   ;; connection per op — a TCP and auth handshake in front of every point read.
   [com.zaxxer/HikariCP "7.1.0"]]

  ;; cljfmt settings mirror vaelii core (no :extra-indents — this adapter uses no
  ;; custom :style/indent macros; those the engine harvests live in core).
  :cljfmt {:indentation?                    true
           :indent-line-comments?           true
           :remove-surrounding-whitespace?  true
           :remove-trailing-whitespace?     true
           :insert-missing-whitespace?      true
           :remove-consecutive-blank-lines? true
           :sort-ns-references?             true}

  :aliases
  ;; Static-analysis gates, mirroring vaelii core's. `lein lint` runs kondo +
  ;; cljfmt + shellcheck + reflect through scripts/lint.sh, which prints one ✓/✗
  ;; line per check and runs ALL of them (not fail-fast), so a red run surfaces
  ;; every problem in one pass (VERBOSE=1 dumps detail). The granular lint-*
  ;; aliases run a single check for a quick one-off. `lein fix` is the rewrite
  ;; half (cljfmt in place); kondo and the reflection ratchet are check-only.
  ;; Needs clj-kondo + shellcheck on PATH; reflect needs the checkouts/vaelii core
  ;; source (scripts/link-checkouts.sh).
  {"lint"            ["shell" "bash" "scripts/lint.sh"]
   "lint-kondo"      ["shell" "clj-kondo" "--lint" "src" "test"]
   "lint-cljfmt"     ["cljfmt" "check"]
   "lint-shellcheck" ["shell" "bash" "scripts/lint-shellcheck.sh"]
   "lint-reflect"    ["shell" "bash" "scripts/check-reflection.sh"]
   "fix"             ["cljfmt" "fix"]
   ;; lint, then (only if green) the in-repo unit suite.
   "gate"            ["do" ["lint"] ["test"]]}

  :profiles
  {;; cljfmt + shell plugins live in :dev (active by default), mirroring core — so
   ;; `lein cljfmt` / `lein lint` work without a profile, while a consumer's POM
   ;; never sees them (a :dev plugin carries Maven scope test).
   ;; slf4j-nop is here and never top-level: HikariCP logs through SLF4J, and a
   ;; provider in :dependencies would be a transitive one for every application
   ;; depending on this adapter, where it can win SLF4J's provider race against the
   ;; host's own backend and silence that instead (core holds the same line).
   :dev  {:dependencies [[nrepl "1.7.0"]
                         [org.slf4j/slf4j-nop "2.0.17"]]
          :plugins [[dev.weavejester/lein-cljfmt "0.16.5"]
                    [lein-shell "0.5.0"]]}
   ;; Tests reach a live Postgres only when one is configured (VAELII_PG_URL or
   ;; the POSTGRES_* triple); otherwise every db-touching test skips with a
   ;; printed reason, so a stray PGHOST never changes what `lein test` means.
   :test {:global-vars {*warn-on-reflection* false}}})

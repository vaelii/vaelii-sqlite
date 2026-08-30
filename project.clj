(defproject com.vaelii/sqlite "0.13.1-SNAPSHOT"
  :description "SQLite targets for vaelii's storage seams. The first is the
                snapshot sink (vaelii.sqlite.snapshot): a SnapshotSink /
                SnapshotSource over a single SQLite file, so a KB image — the index
                projection today, any of io.snapshot's named sections — lands in a
                zero-configuration file as a few bulk transactions and reads back
                the same way. An Apache-2.0 adapter on the SSPL engine, depending on
                core, never depended on by it."
  :license {:name "Apache-2.0" :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :url "https://github.com/vaelii/vaelii-sqlite"
  :scm {:name "git" :url "https://github.com/vaelii/vaelii-sqlite"}
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
   ;; The record store tallies its fetches through `vaelii.impl.profile/record-fetch`,
   ;; which lands in 0.11.0 — so that is the floor, above the 0.9.0 the sink alone needs.
   [com.vaelii/vaelii "0.13.0"]
   ;; the sink's own deps — declared here, not leaned on through core, so a change
   ;; in core's deps cannot break this adapter's load.  Carries the xerial SQLite
   ;; JDBC driver only — no postgresql — so a pure-sqlite run stays minimal.
   [com.github.seancorfield/next.jdbc "1.3.1118"]
   [org.xerial/sqlite-jdbc "3.53.2.0"]
   [com.taoensso/nippy "3.8.1"]]

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
   :dev  {:dependencies [[nrepl "1.7.0"]]
          :plugins [[dev.weavejester/lein-cljfmt "0.16.5"]
                    [lein-shell "0.5.0"]]}
   ;; SQLite needs no server — the suite always runs against a temp file — so the
   ;; test profile only flips reflection off so the test tree doesn't spam it.
   :test {:global-vars {*warn-on-reflection* false}}})

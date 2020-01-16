(ns lupapiste.mongocheck.core
  "Library for running checks on MongoDB data."
  (:require [clojure.set :refer [union]]
            [monger.collection :as mc]))

(defonce ^:private checks (atom {}))

(defn- ->columns [interesting-properties]
  (->> (if (and (coll? (first interesting-properties))
                (= 1 (count interesting-properties)))
         (first interesting-properties)
         interesting-properties)
       (map keyword)
       (set)))

(defn- empty-set? [s]
  (and (set? s) (empty? s)))

(defn- update-columns [columns interesting-properties]
  (let [new-columns (->columns interesting-properties)]
    (if (or (empty-set? columns)      ; columns is empty set = select all properties
            (empty-set? new-columns)) ; no new properties    = select all properties
      #{}
      (union columns new-columns))))

(defn mongocheck
  "Define a named check for a document in given collection.
   Checker function shall be given the document to be checked.
   The function must return nil if the check passes or some kind of an error description.
   Checked document properties can be given to optimize database lookup."
  [collection checker-fn & interesting-properties]
  {:pre [(keyword? collection) (fn? checker-fn)]}
  (swap! checks update collection
         #(-> %
              (update :columns update-columns interesting-properties)
              (update :checks conj checker-fn))))

(defn- errors-for-document [mongo-document checks]
  (->> checks
       (map (fn [f]
              (let [start (System/currentTimeMillis)
                    res (try (f mongo-document)
                             (catch Throwable t
                               (.getMessage t)))]
                [res (str f) (- (System/currentTimeMillis) start)])))
       doall))

(defn- execute-collection-checks [db collection {:keys [columns checks]}]
  (let [times (atom [])
        documents (mc/find-maps db collection {} (zipmap columns (repeat 1)))
        result (->> documents
                    (pmap (fn [mongo-document]
                            (let [[errors fn-name time] (errors-for-document mongo-document checks)]
                              (swap! times conj [time fn-name])
                              (when (seq errors)
                                [(:_id mongo-document) errors]))))
                    (filter seq)
                    (into {}))]
    (->> @times
         (sort-by first >)
         (take 1000)
         (map println)
         dorun)
    result))

(defn execute-checks
  "Execute checks against given db (see monger.core/get-db).
   Returns a map of checked collections.
   Values contain a map of document ids + error messages."
  [db]
  (into {} (pmap (fn [[collection m]] [collection (execute-collection-checks db collection m)]) @checks)))

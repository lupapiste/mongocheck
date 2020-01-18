(ns lupapiste.mongocheck.core
  "Library for running checks on MongoDB data."
  (:require [clojure.set :refer [union]]
            [monger.query :as mq]))

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
    (if (or (empty-set? columns) ; columns is empty set = select all properties
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
                    res   (try (f mongo-document)
                               (catch Throwable t
                                 (.getMessage t)))]
                [res (str f) (- (System/currentTimeMillis) start)])))
       doall))

(defn- execute-collection-checks [db collection {:keys [columns checks]}]
  (let [coll-str (name collection)
        documents (mq/with-collection db coll-str
                    (mq/find {})
                    (mq/fields columns)
                    (mq/sort (array-map :modified -1))
                    (mq/limit 10000))
        results   (->> documents
                       (pmap (fn [mongo-document]
                               (let [[errors fn-name time] (errors-for-document mongo-document checks)]
                                 [time fn-name (when (seq errors)
                                                 [(:_id mongo-document) errors])]))))]
    (try
      (->> results
           (filter (fn [[ms _ _]]
                     (and (number? ms)
                          (> ms 1))))
           (sort-by first >)
           (take 100)
           (map println)
           doall)
      (catch Throwable t
        (println "Error calculating check timing")
        (.printStackTrace t)))
    (->> results
         (keep last)
         (into {}))))

(defn execute-checks
  "Execute checks against given db (see monger.core/get-db).
   Returns a map of checked collections.
   Values contain a map of document ids + error messages."
  [db]
  (->> @checks
       (reduce (fn [acc [collection m]]
                 (assoc acc collection (execute-collection-checks db collection m)))
               {})))

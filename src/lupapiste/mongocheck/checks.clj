(ns lupapiste.mongocheck.checks
  "Collection of commonly needed check")

(defn not-null-property [p]
  "Usage: (mongocheck :someCollection (lupapiste.mongocheck.checks/not-null-property :notNullProperty) :notNullProperty)"
  (fn [mongo-document]
    (when (nil? (get mongo-document p))
      (str (name p) " is null"))))


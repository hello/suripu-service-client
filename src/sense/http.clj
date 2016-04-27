(ns sense.http
  (:require
    [aleph.http :as http])
  (:import
    [com.hello.suripu.service SignedMessage]
    [org.apache.commons.codec.binary Hex]))


(defn sign-protobuf
  "Given a protobuf object and a key, return a valid signed message."
  [proto-message key]
  (let [body (.toByteArray proto-message)
        key-bytes (Hex/decodeHex (.toCharArray key))
        signed (-> body (SignedMessage/sign key-bytes) .get)
        iv (take 16 signed)
        sig (->> signed (drop 16) (take 32))]
    (byte-array (concat body iv sig))))

(defn- post-async
  [url sense-id body & [post-options]]
  (http/post
    url
    (merge
      {:body body
       :headers {"X-Hello-Sense-Id" sense-id}}
      post-options)))

(def ^:private post
  (comp deref post-async))

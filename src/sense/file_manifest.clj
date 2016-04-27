(ns sense.file-manifest
  (:require
    [byte-streams :as bs]
    [clojure.pprint :as pprint]
    [clojure.java.io :as io]
    [clojure.string :as s])
  (:import
    [com.amazonaws ClientConfiguration]
    [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
    [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient]
    [com.amazonaws.services.dynamodbv2.model
      ScanRequest
      ScanResult]
    [com.amazonaws.services.s3 AmazonS3Client]
    [com.amazonaws.services.s3.model
      ObjectMetadata
      PutObjectRequest]
    [com.amazonaws.util Base64]
    [com.google.common.io ByteStreams]
    [com.hello.suripu.api.input
      FileSync$FileManifest
      FileSync$FileManifest$FileDownload
      FileSync$FileManifest$File]
    [com.hello.suripu.core.db FileManifestDynamoDB]
    [org.apache.commons.codec.binary Hex]
    [org.apache.commons.codec.digest DigestUtils]))


;;;;;;;; FileManifest ;;;;;;;;;;;
(defn file-download
  [{:keys [host url sd-card-filename sd-card-path sha1]}]
  (cond-> (FileSync$FileManifest$FileDownload/newBuilder)
    host (.setHost host)
    url (.setUrl url)
    sd-card-filename (.setSdCardFilename sd-card-filename)
    sd-card-path (.setSdCardPath sd-card-path)
    sha1 (.setSha1 sha1)
    :always (.build)))

(defn file-manifest
  [sense-id firmware-version file-download-maps]
  (let [builder (FileSync$FileManifest/newBuilder)]
    (.setSenseId builder sense-id)
    (.setFirmwareVersion builder (int firmware-version))
    (doseq [m file-download-maps]
      (.addFileInfo
        builder
        (.. (FileSync$FileManifest$File/newBuilder)
          (setDownloadInfo (file-download m))
          build)))
    (.build builder)))

(defn get-client
  []
  (let [credentials-provider (DefaultAWSCredentialsProviderChain.)
        client-config (.. (ClientConfiguration.)
                        (withConnectionTimeout 200)
                        (withMaxErrorRetry 1)
                        (withMaxConnections 1000))]
    (doto (AmazonDynamoDBClient. credentials-provider client-config)
      (.setEndpoint "http://dynamodb.us-east-1.amazonaws.com"))))

(defn dao
  [table-name client]
  (FileManifestDynamoDB. client table-name))

(defn get-file-list
  [^FileSync$FileManifest manifest]
  (for [file (.getFileInfoList manifest)
        :let [download-info (.getDownloadInfo ^FileSync$FileManifest$File file)]]
    {:file-name (.getSdCardFilename download-info)
     :path (.getSdCardPath download-info)
     :sha (-> download-info .getSha1 .toByteArray Hex/encodeHex (String.))}))

(defn list-files
  [dao sense-id]
  (let [manifest (.getManifest dao sense-id)]
    (when (.isPresent manifest)
      (get-file-list (.get manifest)))))


;;;;;;;; Scan ;;;;;;;;;
(defn scanifests
  [table-name client]
  (loop [exclusive-start-key nil
         results []]
    (let [request (cond-> (ScanRequest.)
                    :always (.withTableName table-name)
                    :always (.withAttributesToGet ["sense_id" "manifest"])
                    exclusive-start-key (.withExclusiveStartKey exclusive-start-key))
          scan-result (.scan client request)
          last-evaluated-key (.getLastEvaluatedKey scan-result)
          results (->> scan-result
                    .getItems
                    (map #(-> % (get "manifest") .getB .array FileSync$FileManifest/parseFrom))
                    (concat results))]
      (if last-evaluated-key
        (recur last-evaluated-key results)
        results))))

(defn file-stats
  [manifests]
  (->> manifests
    (map get-file-list ,,,)
    (flatten ,,,)
    (group-by (comp (partial clojure.string/join "/") (juxt :path :file-name)) ,,,)
    (map (fn [[k v]] [k (count v)]) ,,,)
    (into {} ,,,)))

(defn sleep-tone?
  [^String file-name]
  (and (.contains file-name "ST0")
       (not (.contains file-name "ST005"))))

(defn sleep-tones
  [file-list]
  (filter (comp sleep-tone? :file-name) file-list))

(defn missing-files?
  [file-count manifest]
  (->> manifest
    get-file-list
    sleep-tones
    count
    (not= file-count)))

(defn pprint-file-stats
  [manifests]
  (->> manifests
    file-stats
    (filter (fn [[k v]] (.contains ^String k "ST0")))
    (sort-by first)
    (pprint/pprint)))

(defn sd-card-stats
  [manifests]
  (let [free-mems (map #(.. % getSdCardSize getFreeMemory) manifests)
        failures (->> manifests
                    (filter #(.. % getSdCardSize getSdCardFailure))
                    (map #(.getSenseId %)))
        num-failures (count failures)]
    {:free-mems free-mems
     :failures failures
     :num-failures num-failures}))

(defn sense-id
  [^FileSync$FileManifest manifest]
  (.getSenseId manifest))

(defn sd-failure?
  [^FileSync$FileManifest manifest]
  (.. manifest getSdCardSize getSdCardFailure))

(defn chris-query
  [manifests]
  (let [missing-files (->> manifests
                        (filter (partial missing-files? 11))
                        (map sense-id)
                        set)
        sd-failures (->> manifests
                      (filter sd-failure?)
                      (map sense-id)
                      set)]
    {:missing-files missing-files
     :sd-failures sd-failures
     :difference (clojure.set/difference missing-files sd-failures)}))

;;;;;;;; SHA ;;;;;;;;;;
(defprotocol Sha1
  (sha1 [this]))

(extend-protocol Sha1
  (Class/forName "[B")
  (sha1 [bytes]
    (.digest (java.security.MessageDigest/getInstance "sha1") bytes))

  String
  (sha1 [filename]
    (with-open [in (io/input-stream filename)]
      (-> in bs/to-byte-array sha1))))

(defn encode-hex
  [^bytes bytes]
  (String. (Hex/encodeHex bytes)))



;;;;;;;;; File Info ;;;;;;;;;;;
(def file-info-insert-template
  "INSERT INTO file_info (sort_key, firmware_version, type, path, sha, uri, preview_uri, name, is_public)
  VALUES ({sort-key}, 1, 'SLEEP_SOUND', '/SLPTONES/{file-name}', '{sha}', '{s3-path}{s3-name}', '', '{name}', false);")

(def sense-file-info-insert-template
  "INSERT INTO sense_file_info (sense_id, file_info_id)
  VALUES ('{sense-id}', {file-info-id});")

(def file-info-update-template
  "UPDATE file_info SET sha='{sha}' WHERE uri='{s3-path}{s3-name}';")

(def file-info-update-template-with-s3
  "UPDATE file_info SET sha='{sha}', uri='{s3-path}{s3-name}' WHERE name='{name}';")

(defn replace-statement
  [sym]
  (let [regex (re-pattern (str "\\{" (name sym) "\\}"))]
    `(s/replace ~regex (str ~sym))))

(defmacro interpolate
  [template & syms]
  (let [replace-statements (map replace-statement syms)]
    `(-> ~template ~@replace-statements)))

(defn generate-file-info-insert
  [file-path sort-key name s3-path]
  (let [sha (-> file-path sha1 encode-hex)
        s3-name (-> file-path io/file .getName)
        file-name (s/upper-case s3-name)]
    (println (interpolate file-info-insert-template sort-key file-name sha name s3-path s3-name))))

(defn generate-sense-file-info-insert
  [sense-id file-info-id]
  (println (interpolate sense-file-info-insert-template sense-id file-info-id)))

(defn mk-tuple-strings
  [tuples]
  (->> tuples
    (map (fn [[x y]] (interpolate "({{x}, {y}})" x y)))
    (s/join ", ")))

(defn generate-sense-file-info-inserts
  [sense-ids file-info-ids]
  (let [tuples (for [sense-id sense-ids
                     file-info-id file-info-ids]
                [(str "'" sense-id "'") file-info-id])
        tuple-string (mk-tuple-strings tuples)]
    (interpolate
      "INSERT INTO sense_file_info (sense_id, file_info_id) VALUES {tuple-string};"
      tuple-string)))



;;;;;;;;;; S3 upload ;;;;;;;;;;;;

; final ObjectMetadata md = new ObjectMetadata();
; md.setContentLength(byteLength);
; final byte[] content = ByteStreams.toByteArray(new FileInputStream(file));
; byte[] resultByte = DigestUtils.md5(content);
; final String streamMD5 = Base64.encodeAsString(resultByte);
; md.setContentMD5(streamMD5);
; final Map<String, String> userMd = Maps.newHashMap();
; final String sha1 = DigestUtils.sha1Hex(content);
; userMd.put("sha1", sha1);
; md.setUserMetadata(userMd);
; // 9 = ST008.raw
; final String suffix = filename.substring(filename.length() - 9);
; final String key = String.format("sleep-tones-raw/%s", suffix);
; final PutObjectRequest req = new PutObjectRequest("hello-audio", key, limitStream, md);

(def file-names
  ["Brown noise"
   "Outer space"
   "Wind"
   "Fire"
   "Ocean"
   "Rain"
   "White noise"
   "River"
   "Chimes"
   "Ambient"
   "Strings 1"
   "Strings 2"])

(defn md5
  [bytes]
  (DigestUtils/md5 bytes))

(defn s3
  []
  (AmazonS3Client.))

(defn to-byte-array
  [file]
  (with-open [in (io/input-stream file)]
    (bs/to-byte-array in)))

(defn upload
  [s3-client s3-bucket s3-prefix template sort-key name file-path]
  (let [file (io/file file-path)
        file-size (.length file)
        content (to-byte-array file)
        sha (DigestUtils/sha1Hex content)
        user-meta {"sha1" sha}
        object-meta (doto (ObjectMetadata.)
                      (.setContentLength file-size)
                      (.setContentMD5 (-> content md5 Base64/encodeAsString))
                      (.setUserMetadata user-meta))
        s3-name (.getName file)
        file-name (s/upper-case s3-name)
        s3-key (interpolate "{s3-prefix}/{s3-name}" s3-prefix s3-name)
        s3-path (interpolate "s3://{s3-bucket}/{s3-prefix}/" s3-bucket s3-prefix)]
    #_(prn s3-key s3-path)
    #_(with-open [is (io/input-stream file)
                limit-stream (ByteStreams/limit is file-size)]
      (.putObject
        s3-client
        (PutObjectRequest. s3-bucket s3-key limit-stream object-meta)))
    (println (interpolate template sort-key file-name sha name s3-path s3-name))))


(defn rando
  []
  (rand-int Integer/MAX_VALUE))

(defn timeit
  [f]
  (let [start (System/nanoTime)
        ret (f)
        end (System/nanoTime)]
    (/ (double (- end start)) 1000000.0)))

(defn benchmark
  [f n]
  (let [p (promise)
        futures (mapv (fn [_] (future @p (timeit f))) (range n))]
    (deliver p true)
    (mapv deref futures)))

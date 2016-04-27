(defproject suripu-service-client "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [aleph "0.4.1-beta2"]
                 [com.hello.suripu/suripu-service "0.6.135"]
                 [com.hello/messeji-standalone "0.2.11"]]
  :plugins [[s3-wagon-private "1.2.0"]]
  :repositories [["releases" {:url "s3p://hello-maven/release/"
                              :username :env/aws_access_key_id
                              :passphrase :env/aws_secret_key
                              :sign-releases false}]])

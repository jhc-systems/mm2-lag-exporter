version=2.1.0
mvn clean deploy -P  jhc.release
podman build . -t eu.gcr.io/$TF_VAR_project/mm-lag-exporter:$version
podman push eu.gcr.io/$TF_VAR_project/mm-lag-exporter:$version

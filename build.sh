version=2.0.0
docker build . -t eu.gcr.io/$TF_VAR_project/mm-lag-exporter:$version
docker push eu.gcr.io/$TF_VAR_project/mm-lag-exporter:$version

    gcloud beta composer environments create test-environment \
        --location us-central1 \
        --zone us-central1-f \
        --machine-type n1-standard-2 \
        --image-version composer-latest-airflow-x.y.z \
        --labels env=beta  
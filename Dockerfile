FROM quay.io/astronomer/ap-airflow:2.0.0-2-buster-onbuild

ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name":"partner_a","api_secret":"mysecret","path":"/tmp/partner_a"}'
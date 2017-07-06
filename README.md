# CMS-Dataset-Popularity

## Continuous Integration

Gitlab CI runs Docker containers to run checkstyle and tests. Then it creates a Docker image containing the Pyspark jobs.

Command used to create Docker image for pylint and tests:

```
docker build -f ./docker/Dockerfile-base -t gitlab-registry.cern.ch/awg/cms-dataset-popularity:test .
docker push gitlab-registry.cern.ch/awg/cms-dataset-popularity:test
```

The file ``./docker/Dockerfile-ci`` contains the instruction to build the image containing the PySpark job. It is built automatically in the Gitlab pipeline at the stage "docker" specified in the ``.gitlab-ci.yml`` file.

Gitlab CI is triggered by commit in the ``deploy`` branch.

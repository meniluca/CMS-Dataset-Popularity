# CMS-Dataset-Popularity

Missing repository description.

## Continuous Integration

Gitlab CI runs Docker containers for checkstyle control and tests, then it creates a new Docker image containing the Pyspark jobs.  The procedure is specified in the ``.gitlab-ci.yml`` file and it is automatically triggered by new commits in the ``deploy`` branch.

Commands used to create Docker image for pylint and tests are:

```bash
docker build -f ./docker/Dockerfile-ci -t gitlab-registry.cern.ch/awg/cms-dataset-popularity:test .
docker push gitlab-registry.cern.ch/awg/cms-dataset-popularity:test
```

Instead the ``./docker/Dockerfile-base`` file is used by the Gitlab CI to create the image containing the PySpark job.

## Docker commands

You can use the Docker container to simulate CI locally before pushing:

* To run pylint:

```bash
docker run -v $PWD:/opt/cms-datapop/job gitlab-registry.cern.ch/awg/cms-dataset-popularity:test ../pyenv/bin/pylint --rcfile=conf/.rcfile src/main.py src/spark/util.py src/datapop/phedex.py
```

* To run tests with nosetests:

```bash
docker run -v $PWD:/opt/cms-datapop/job gitlab-registry.cern.ch/awg/cms-dataset-popularity:test ../pyenv/bin/nosetests -v -s tests/main.py
```

## How to submit the job

* From node with docker (add link)
* From lxplus

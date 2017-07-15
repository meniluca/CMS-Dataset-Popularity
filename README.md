# CMS-Dataset-Popularity

Missing repository description.

## Continuous Integration

Gitlab CI runs Docker containers for checkstyle control and tests, then it creates a new Docker image with the Pyspark jobs.  The procedure is specified in the ``.gitlab-ci.yml`` file and it is automatically triggered by new commits in the ``deploy`` branch.

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
docker run -v $PWD:/opt/cms-datapop/job gitlab-registry.cern.ch/awg/cms-dataset-popularity:test ../pyenv/bin/pylint --rcfile=conf/.rcfile src/main.py src/spark/util.py src/datapop/phedex.py src/datapop/jobs.py
```

* To run tests with nosetests:

```bash
docker run -v $PWD:/opt/cms-datapop/job gitlab-registry.cern.ch/awg/cms-dataset-popularity:test ../pyenv/bin/nosetests -v -s tests/main.py
```

## How to submit the job

* From node with docker (add link)

```
kinit $USER
KRB5CCNAME=$(klist|grep Ticket|awk -F": " '{print $2}')
docker run -it --net=host -e KRB5CCNAME=$KRB5CCNAME -e AFS_USER=$USER \
    -v /tmp:/tmp -v /afs:/afs \
    -v /etc/hadoop/conf/:/etc/hadoop/conf -v /etc/spark/conf:/opt/spark/conf \
    gitlab-registry.cern.ch/awg/cms-dataset-popularity:latest
```

* From lxplus

# Makefile used to build docker images for Connect File Pulse

CONNECT_VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)

REPOSITORY = streamthoughts
IMAGE = kafka-connect-file-pulse

.SILENT:

all: build-images clean-build

clean-containers:
	echo "Cleaning containers \n========================================== ";

clean-images:
	echo "Cleaning images \n========================================== ";
	for image in `docker images -qf "label=io.streamthoughts.docker.name"`; do \
	    echo "Removing image $${image} \n==========================================\n " ; \
        docker rmi -f $${image} || exit 1 ; \
    done

clean-build:
	echo "Cleaning build directory \n========================================== ";
	rm -rf ./docker-build;

build-images:
	echo "Building image \n========================================== ";
	echo "CONNECT_VERSION="$(CONNECT_VERSION)
	echo "==========================================\n "
	mvn clean package && \
	docker build \
	-t ${REPOSITORY}/${IMAGE}:latest . || exit 1 ;
	docker tag ${REPOSITORY}/${IMAGE}:latest ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION} || exit 1 ;

clean: clean-containers clean-images clean-build

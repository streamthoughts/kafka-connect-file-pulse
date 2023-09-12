# Makefile used to build docker images for Connect File Pulse
SHELL = bash

VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
COMMIT := $(shell git rev-parse --short HEAD)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
DATE := $(shell date --rfc-3339=seconds)

REPOSITORY = streamthoughts
IMAGE = kafka-connect-file-pulse
PROFILES = gcs aws azure local
DISTRIBUTION = ${REPOSITORY}-${IMAGE}-${VERSION}.zip
DOCKER_PATH = ./docker

.SILENT:

.PHONY: clean clean-build clean-containers clean-containers build-dist print-info build-images docker-build push-images

all: clean-images build-images clean-build

clean-containers:
	echo -e "Cleaning containers \n========================================== ";

clean-images:
	echo -e "Cleaning images \n========================================== ";
	for image in `docker images -qf "label=org.opencontainers.image.title=streamthoughts-kafka-connect-file-pulse"`; do \
	    echo "Removing image $${image} \n==========================================\n " ; \
        docker rmi -f $${image} || exit 1 ; \
    done

clean-build:
	echo -e "Cleaning build directory \n========================================== ";
	rm -rf ./docker/*.zip;

clean: clean-containers clean-images clean-build

print-info:
	echo -e "\n==========================================\n";
	echo "CONNECT_VERSION="$(VERSION);
	echo "GIT_COMMIT="$(COMMIT);
	echo "GIT_BRANCH="$(BRANCH);
	echo "MVN_PROFILE="$$MVN_PROFILE;
	echo -e "\n==========================================\n";

build-dist: print-info
	if [[ ! -z "$$MVN_PROFILE" ]]; then \
		./mvnw clean package -B -q -DskipTests -P dist,$$MVN_PROFILE; \
	else \
		./mvnw clean package -B -q -DskipTests -P dist,all; \
	fi

docker-build: build-dist
	rm -rf ${DOCKER_PATH}/*.zip; \
	cp ./connect-file-pulse-plugin/target/components/packages/${DISTRIBUTION} ./${DOCKER_PATH}/; \
	export FULL_IMAGE_TAG_LATEST=${REPOSITORY}/${IMAGE}:latest; \
	export FULL_IMAGE_TAG_VERSION=${REPOSITORY}/${IMAGE}:${VERSION}; \
	export FULL_IMAGE_TAG_BRANCH=${REPOSITORY}/${IMAGE}:${BRANCH}; \
	if [[ ! -z "$$MVN_PROFILE" ]]; then \
		FULL_IMAGE_TAG_LATEST=$$FULL_IMAGE_TAG_LATEST-$$MVN_PROFILE; \
		FULL_IMAGE_TAG_VERSION=$$FULL_IMAGE_TAG_VERSION-$$MVN_PROFILE; \
		FULL_IMAGE_TAG_BRANCH=$$FULL_IMAGE_TAG_BRANCH-$$MVN_PROFILE; \
	fi; \
	docker build \
		--compress \
		--rm \
		--build-arg VERSION="${VERSION}" \
		--build-arg COMMIT="${COMMIT}" \
		--build-arg BRANCH="${BRANCH}" \
		--build-arg CREATED="${DATE}" \
		-f ./docker/Dockerfile \
		-t $$FULL_IMAGE_TAG_LATEST ${DOCKER_PATH} || exit 1 ; \
	docker tag $$FULL_IMAGE_TAG_LATEST $$FULL_IMAGE_TAG_VERSION || exit 1 ; \
	docker tag $$FULL_IMAGE_TAG_LATEST $$FULL_IMAGE_TAG_BRANCH || exit 1 ;

build-images: docker-build
	for PROFILE in $(PROFILES); do MVN_PROFILE=$$PROFILE $(MAKE) docker-build; done

push-images: print-info
	docker push ${REPOSITORY}/${IMAGE}:${VERSION} || exit 1 ;
	docker push ${REPOSITORY}/${IMAGE}:latest || exit 1 ;
	for PROFILE in $(PROFILES); do\
		docker push ${REPOSITORY}/${IMAGE}:${VERSION}-$$PROFILE || exit 1 ;\
	done

changelog:
	./mvnw jreleaser:changelog -Prelease --file pom.xml

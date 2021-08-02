# Makefile used to build docker images for Connect File Pulse
SHELL = bash

CONNECT_VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

REPOSITORY = streamthoughts
IMAGE = kafka-connect-file-pulse
PROFILES = gcs aws azure local

.SILENT:

.PHONY: clean clean-build clean-containers clean-containers build-images docker-build push-images

all: build-images clean-build

clean-containers:
	echo -e "Cleaning containers \n========================================== ";

clean-images:
	echo -e "Cleaning images \n========================================== ";
	for image in `docker images -qf "label=io.streamthoughts.docker.name"`; do \
	    echo "Removing image $${image} \n==========================================\n " ; \
        docker rmi -f $${image} || exit 1 ; \
    done

clean-build:
	echo -e "Cleaning build directory \n========================================== ";
	rm -rf ./docker-build;

clean: clean-containers clean-images clean-build

print-info:
	echo -e "\n==========================================\n";
	echo "CONNECT_VERSION="$(CONNECT_VERSION);
	echo "GIT_COMMIT="$(GIT_COMMIT);
	echo "GIT_BRANCH="$(GIT_BRANCH);
	echo "MVN_PROFILE="$$MVN_PROFILE;
	echo -e "\n==========================================\n";

mvn-package: print-info
	if [[ ! -z "$$MVN_PROFILE" ]]; then \
		./mvnw clean package -B -Dmaven.test.skip=true -P $$MVN_PROFILE; \
	else \
		./mvnw clean package -B -Dmaven.test.skip=true; \
	fi

docker-build: mvn-package
	export FULL_IMAGE_TAG_LATEST=${REPOSITORY}/${IMAGE}:latest; \
	export FULL_IMAGE_TAG_VERSION=${REPOSITORY}/${IMAGE}:${CONNECT_VERSION}; \
	export FULL_IMAGE_TAG_BRANCH=${REPOSITORY}/${IMAGE}:${GIT_BRANCH}; \
	if [[ ! -z "$$MVN_PROFILE" ]]; then \
		FULL_IMAGE_TAG_LATEST=$$FULL_IMAGE_TAG_LATEST-$$MVN_PROFILE; \
		FULL_IMAGE_TAG_VERSION=$$FULL_IMAGE_TAG_VERSION-$$MVN_PROFILE; \
		FULL_IMAGE_TAG_BRANCH=$$FULL_IMAGE_TAG_BRANCH-$$MVN_PROFILE; \
	fi; \
	docker build \
		--build-arg connectFilePulseVersion=${CONNECT_VERSION} \
		--build-arg connectFilePulseCommit=${GIT_COMMIT} \
		--build-arg connectFilePulseBranch=${GIT_BRANCH} \
		-t $$FULL_IMAGE_TAG_LATEST . || exit 1 ; \
	docker tag $$FULL_IMAGE_TAG_LATEST $$FULL_IMAGE_TAG_VERSION || exit 1 ; \
	docker tag $$FULL_IMAGE_TAG_LATEST $$FULL_IMAGE_TAG_BRANCH || exit 1 ;

build-images: docker-build
	for PROFILE in $(PROFILES); do MVN_PROFILE=$$PROFILE $(MAKE) docker-build; done

push-images: print-info
	docker push ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION} || exit 1 ;
	docker push ${REPOSITORY}/${IMAGE}:latest || exit 1 ;
	for PROFILE in $(PROFILES); do\
		docker push ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION}-$$PROFILE || exit 1 ;\
	done

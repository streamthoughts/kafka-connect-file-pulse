# Makefile used to build docker images for Connect File Pulse

CONNECT_VERSION := $(shell mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

REPOSITORY = streamthoughts
IMAGE = kafka-connect-file-pulse
PROFILES = gcs aws azure local

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
	echo "Building Docker images \n========================================== ";
	echo "CONNECT_VERSION="$(CONNECT_VERSION);
	echo "GIT_COMMIT="$(GIT_COMMIT);
	echo "GIT_BRANCH="$(GIT_BRANCH);
	echo "==========================================\n ";
	./mvnw clean package -B -Dmaven.test.skip=true -P one-for-all  && \
	docker build \
    --build-arg connectFilePulseVersion=${CONNECT_VERSION} \
    --build-arg connectFilePulseCommit=${GIT_COMMIT} \
    --build-arg connectFilePulseBranch=${GIT_BRANCH} \
	-t ${REPOSITORY}/${IMAGE}:latest . || exit 1 ;
	docker tag ${REPOSITORY}/${IMAGE}:latest ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION} || exit 1 ;

	for PROFILE in $(PROFILES); do\
        echo "Building Docker images \n========================================== ";\
		echo "PROFILE=$$PROFILE";\
		echo "==========================================\n ";\
		./mvnw clean package -B -Dmaven.test.skip=true -P $$PROFILE && \
		docker build \
		--build-arg connectFilePulseVersion=${CONNECT_VERSION} \
		--build-arg connectFilePulseCommit=${GIT_COMMIT} \
		--build-arg connectFilePulseBranch=${GIT_BRANCH} \
		-t ${REPOSITORY}/${IMAGE}:latest-$$PROFILE . || exit 1 ; \
		docker tag ${REPOSITORY}/${IMAGE}:latest-$$PROFILE ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION}-$$PROFILE || exit 1 ;\
	done

push-images:
	echo "Pushing Docker images \n========================================== ";
	echo "CONNECT_VERSION="$(CONNECT_VERSION);
	echo "GIT_COMMIT="$(GIT_COMMIT);
	echo "GIT_BRANCH="$(GIT_BRANCH);
	echo "==========================================\n ";
	docker push ${REPOSITORY}/${IMAGE}:latest ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION} || exit 1 ;
	docker push ${REPOSITORY}/${IMAGE}:latest ${REPOSITORY}/${IMAGE}:latest || exit 1 ;
	for PROFILE in $(PROFILES); do\
		docker push ${REPOSITORY}/${IMAGE}:${CONNECT_VERSION}-$$PROFILE || exit 1 ;\
	done

clean: clean-containers clean-images clean-build

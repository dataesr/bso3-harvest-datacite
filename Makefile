DOCKER_IMAGE_NAME=dataesr/bso3-harvest-datacite
CURRENT_VERSION=$(shell cat project/__init__.py | cut -d "'" -f 2)

docker-build:
	@echo Building a new docker image
	docker build -t $(DOCKER_IMAGE_NAME):$(CURRENT_VERSION) -t $(DOCKER_IMAGE_NAME):latest -f ./Dockerfiles/prod/. .
	@echo Docker image built

docker-push:
	@echo Pushing a new docker image
	docker push $(DOCKER_IMAGE_NAME):$(CURRENT_VERSION)
	docker push $(DOCKER_IMAGE_NAME):latest
	@echo Docker image pushed

docker-compose-up:
	@echo Launching the project ecosystem...
	docker-compose up --build -d -V
	@echo Project ecosystem launched

docker-compose-down:
	@echo Stopping the project ecosystem
	docker-compose down
	@echo Project ecosystem stopped

docker-compose-restart: docker-compose-down docker-compose-up

install:
	@echo Installing dependencies...
	pip install -r requirements.txt
	@echo End of dependencies installation

release:
	echo "__version__ = '$(VERSION)'" > project/__init__.py
	git commit -am '[release] version $(VERSION)'
	git tag $(VERSION)
	@echo If everything is OK, you can push with tags i.e. git push origin main --tags

unit-tests:
	python3 -m pytest --disable-warnings tests

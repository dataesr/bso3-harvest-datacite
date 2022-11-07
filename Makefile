DOCKER_IMAGE_NAME=dataesr/bso3-harvest-datacite
CURRENT_VERSION=$(shell cat project/__init__.py | cut -d "'" -f 2)

docker-build:
	@echo Building a new docker image
	docker build -t $(DOCKER_IMAGE_NAME):$(CURRENT_VERSION) -t $(DOCKER_IMAGE_NAME):latest -f ./Dockerfiles/prod/Dockerfile .
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
	pip install -r requirements-dev.txt
	@echo End of dependencies installation

release:
	echo "__version__ = '$(VERSION)'" > project/__init__.py
	git commit -am '[release] version $(VERSION)'
	git tag $(VERSION)
	@echo If everything is OK, you can push with tags i.e. git push origin main --tags

lint: lint-syntax lint-style

lint-style:
	@echo Checking style errors - PEP 8
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=200 --statistics

lint-syntax:
	@echo Checking syntax errors
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

tests: unit-tests integration-tests

unit-tests:
	@echo Unit Testing
	python3.8 -m pytest --disable-warnings tests/unit_test

integration-tests:
	@echo Integration Testing
	python3.8 -m pytest --disable-warnings tests/integration_test

coverage-report:
	@echo Calculating coverage
	coverage run -m pytest --disable-warnings tests/
	@echo Show report
	coverage report -m

show-pydoc:
	@echo launching pydoc server
	python3.8 -m pydoc -b -p 46809 -n 0.0.0.0

show-pdoc3:
	pdoc3 --http 0.0.0.0:46809 adapters application domain project
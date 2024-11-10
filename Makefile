install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m unittest tests/test_pyspark.py

format:	
	black src/*.py tests/*.py

lint:
	ruff check src/*.py tests/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < .devcontainer/Dockerfile

refactor: format lint

deploy:

	docker build -f .devcontainer/Dockerfile -t arko_pyspark:latest .

	docker rm -f arko_pyspark

	docker run -d --name arko_pyspark -p 80:80 arko_pyspark:latest

	echo "Deployment completed."
		
all: install format lint test deploy

install:
	@echo "Creating a virtual environment..."
	python3 -m venv venv && source venv/bin/activate &&\
		pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib --cov-report=html test_*.py
	@echo "Coverage report generated in ./htmlcov/index.html"

format:
	black .

lint:
	@echo "Running Ruff for linting..."
	ruff check --fix *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

validate: format lint

deploy:
	@echo "Deploy script goes here"

all: install lint test format


# install:
# 	pip install --upgrade pip &&\
# 		pip install -r requirements.txt

# test:
# 	python -m pytest -vv --cov=main --cov=mylib test_*.py

# format:	
# 	black *.py 

# lint:
# 	#disable comment to test speed
# 	#pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py
# 	#ruff linting is 10-100X faster than pylint
# 	ruff check *.py mylib/*.py

# container-lint:
# 	docker run --rm -i hadolint/hadolint < Dockerfile

# refactor: format lint

# deploy:
# 	#deploy goes here
		
# all: install lint test format deploy

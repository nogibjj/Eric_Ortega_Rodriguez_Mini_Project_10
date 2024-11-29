# Use bash for compatibility with `source` if needed
SHELL := /bin/bash

install:
	@echo "Creating a virtual environment..."
	python3 -m venv venv && . venv/bin/activate &&\
		pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	@echo "Running Ruff for linting..."
	. venv/bin/activate && ruff check --fix *.py mylib/*.py


# Run tests with pytest and generate a coverage report
test:
	@echo "Running tests with coverage..."
	python -m pytest -vv --cov=main --cov=mylib --cov-report=html test_*.py
	@echo "Coverage report generated in ./htmlcov/index.html"

# Format Python code using black
format:
	@echo "Formatting code with black..."
	black .

# Lint Python code using ruff
lint:
	@echo "Running Ruff for linting..."
	ruff check --fix *.py mylib/*.py

# Lint the Dockerfile using hadolint
container-lint:
	@echo "Linting Dockerfile with hadolint..."
	docker run --rm -i hadolint/hadolint < Dockerfile

# Run both formatting and linting
validate: format lint

# Placeholder for deployment steps
deploy:
	@echo "Deployment script goes here"

# Run all necessary steps
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

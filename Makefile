PYTHON_MAJOR_VERSION=$(shell python -c "import sys; print(sys.version_info.major)")

MAIN_REQUIREMENTS=requirements.txt
EXTENDED_REQUIREMENTS=requirements-py$(PYTHON_MAJOR_VERSION).txt



test:
	@env PYTHONHASHSEED=random PYTHONPATH=. nosetests --nocapture --with-coverage --cover-package=memcrashed --cover-erase --with-yanc --with-xtraceback tests/

build: test
	@echo Running syntax check...
	-@flake8 . --ignore=E501

install:
	pip install -r $(MAIN_REQUIREMENTS) --use-mirrors
	pip install -r requirements-py$(PYTHON_MAJOR_VERSION).txt --use-mirrors

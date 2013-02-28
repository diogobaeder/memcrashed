PYTHON_MAJOR_VERSION=$(shell python -c "import sys; print(sys.version_info.major)")

MAIN_REQUIREMENTS=requirements.txt
EXTENDED_REQUIREMENTS=requirements-py$(PYTHON_MAJOR_VERSION).txt



build: test lint

test: test-py$(PYTHON_MAJOR_VERSION)

test-py2:
	@echo Running tests for Python 2...
	@env PYTHONHASHSEED=random PYTHONPATH=. nosetests --with-coverage --cover-package=memcrashed --cover-erase --with-yanc --with-xtraceback --with-html --html-file=nosetests.html --with-xunit tests/

test-py3:
	@echo Running tests for Python 3...
	@env PYTHONHASHSEED=random PYTHONPATH=. nosetests --with-coverage --cover-package=memcrashed --cover-erase --with-yanc --with-xtraceback tests/

lint:
	@echo Running syntax check...
	-@flake8 . --ignore=E501

install:
	pip install -r $(MAIN_REQUIREMENTS) --use-mirrors
	pip install -r requirements-py$(PYTHON_MAJOR_VERSION).txt --use-mirrors

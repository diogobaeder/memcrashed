test:
	@env PYTHONHASHSEED=random PYTHONPATH=. nosetests --with-coverage --cover-package=memcrashed --cover-erase --with-yanc --with-xtraceback tests/

build: test
	@echo Running syntax check...
	-@flake8 . --ignore=E501

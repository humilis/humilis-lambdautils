PIP := .env/bin/pip
TOX := .env/bin/tox
PYTHON := .env/bin/python

# create a python2.7 virtualenv (same version available in AWS Lambda)
.env:
	virtualenv .env -p python2.7
	$(PIP) install tox

# set up development environment
develop: .env
	$(PIP) install -e . -r requirements-dev.txt

# run test suite
test: .env
	rm -rf .tox
	$(TOX)

# remove virtualenv and layers dir
clean:
	rm -rf .env .tox
	rm -rf tests/__pycache__ lambdautils/__pycache__ lambdautils/*.pyc tests/*.pyc

# upload to Pypi
pypi: develop
	$(PYTHON) setup.py sdist upload

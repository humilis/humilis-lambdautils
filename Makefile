PIP := .env/bin/pip
TOX := .env/bin/tox
PYTHON := .env/bin/python

# create a python2.7 virtualenv (same version available in AWS Lambda)
.env:
	virtualenv .env -p python2.7

# set up development environment
develop: .env
	$(PIP) install -r requirements-dev.txt

# run test suite
test: develop
	rm -rf .tox
	$(TOX)

# remove virtualenv and layers dir
clean:
	rm -rf .env .tox

# upload to Pypi
pypi: develop
	$(PYTHON) setup.py sdist upload

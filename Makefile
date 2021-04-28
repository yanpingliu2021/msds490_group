setup:
	mkdir -p ./.venv && python3 -m venv ./.venv

env:
	#Show information about environment
	which python3
	python3 --version
	which pytest
	which pylint

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

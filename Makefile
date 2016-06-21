.PHONY : html

html :
	PYTHONPATH=".:$${PYTHONPATH}" sphinx-build -b html doc doc/_build

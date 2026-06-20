make:
    echo "Welcome to Project 'necroflow_gui'"

upload_test_pypi:
    twine check dist/*
    python -m pip install --upgrade twine
    twine upload --repository testpypi dist/*

upload_pypi:
    twine check dist/*
    python -m pip install --upgrade twine
    twine upload dist/* 

ve_necroflow_gui:
    python3 -m venv ve_necroflow_gui

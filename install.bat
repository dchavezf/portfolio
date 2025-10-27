python -m venv .venv
.venv\Scripts\activate
rem pip freeze > requirements.txt
pip install -r requirements.txt
python.exe -m pip install --upgrade pip
@echo off
cd /d "%~dp0"
if not defined CONDA_ACTIVATE set "CONDA_ACTIVATE=C:\Users\smartapps\AppData\Local\anaconda3\Scripts\activate.bat"
call "%CONDA_ACTIVATE%" MecanizadosZFER
python "%~dp0main.py" 3
set "RC=%errorlevel%"
call conda deactivate
exit /b %RC%

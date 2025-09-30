@echo off
rem ==== run.bat ====
rem Запускает sftp_pull_to_zip.py с Python из локального виртуального окружения (.venv).
rem Работает при запуске двойным щелчком, и при вызове из cmd.

REM Определяем базовую директорию (где находится этот bat)
SETLOCAL
SET "BASE=%~dp0"
REM Убираем завершающий слэш (иногда %~dp0 имеет завершающий \)
IF "%BASE:~-1%"=="\" SET "BASE=%BASE:~0,-1%"

REM Путь к python в venv
SET "PYTHON=%BASE%\.venv\Scripts\python.exe"

REM Если python не найден — сообщаем и даём возможность запустить системный python
IF NOT EXIST "%PYTHON%" (
    echo [WARN] Локальный python не найден в %PYTHON%.
    echo Попробую использовать python из PATH...
    SET "PYTHON=python"
)

REM (Опция) можно указать дополнительные аргументы по умолчанию (отредактируйте при необходимости)
REM Например: SET "DEFAULT_ARGS=--host 192.168.89.11 --user myuser --archive"
SET "DEFAULT_ARGS="

REM Запуск
echo [INFO] Запуск: "%PYTHON%" "%BASE%\sftp_pull_to_zip.py" %DEFAULT_ARGS% %*
"%PYTHON%" "%BASE%\sftp_pull_to_zip.py" %DEFAULT_ARGS% %%

REM при желании: pause чтобы окно не закрылось автоматически при ошибке
REM pause

ENDLOCAL

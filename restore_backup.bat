@echo off
rem ==== run.bat ====
rem Запускает push\push_archive_to_vm.py и подставляет путь к архиву из папки "archive" рядом с .bat

setlocal EnableExtensions

REM ---- База и рабочая директория ----
set "BASE=%~dp0"
if "%BASE:~-1%"=="\" set "BASE=%BASE:~0,-1%"
pushd "%BASE%" 1>nul 2>nul || (
  echo [ERROR] Не удалось перейти в каталог скрипта: "%BASE%".
  endlocal & exit /b 1
)

REM ---- Выбор интерпретатора Python ----
set "VENV_PY=%BASE%\venv\Scripts\python.exe"
set "PY_CMD="
if exist "%VENV_PY%" (
  set "PY_CMD="%VENV_PY%""
) else (
  set "PY_CMD=python"
)

REM ---- Определим архив ----
set "ARCHIVE_DIR=%BASE%"
set "ARCHIVE_FILE="

if not exist "%ARCHIVE_DIR%" (
  echo [ERROR] Папка с архивами не найдена: "%ARCHIVE_DIR%"
  goto :fail
)

REM Сначала ищем последние *.zip по дате (самый новый первый)
for /f "delims=" %%F in ('dir /b /a-d /o-d "%ARCHIVE_DIR%\*.zip" 2^>nul') do (
  set "ARCHIVE_FILE=%%~fF"
  goto :have_archive
)

REM Если zip'ов нет — берём любой самый новый файл
for /f "delims=" %%F in ('dir /b /a-d /o-d "%ARCHIVE_DIR%\*" 2^>nul') do (
  set "ARCHIVE_FILE=%%~fF"
  goto :have_archive
)

echo [ERROR] В папке "%ARCHIVE_DIR%" не найдено ни одного файла.
goto :fail

:have_archive
echo [INFO] Выбран архив: "%ARCHIVE_FILE%"

REM ---- Скрипт Python ----
set "SCRIPT=%BASE%\push\push_archive_to_vm.py"
if not exist "%SCRIPT%" (
  echo [ERROR] Не найден файл скрипта: "%SCRIPT%"
  goto :fail
)

REM ---- Запуск (прокидываем все дополнительные аргументы пользователя: %*) ----
echo [INFO] Python: %PY_CMD%
if "%PY_CMD%"=="py -3" (
  py -3 -X utf8 -u "%SCRIPT%" --archive "%ARCHIVE_FILE%" %*
) else (
  %PY_CMD% -X utf8 -u "%SCRIPT%" --archive "%ARCHIVE_FILE%" %*
)
set "RC=%ERRORLEVEL%"
goto :epilog

:fail
set "RC=2"

:epilog
REM Пауза при ошибке если запуск из Проводника (двойной клик)
set "FROM_EXPLORER="
echo %CMDCMDLINE% | find /I "/c" >nul && set "FROM_EXPLORER=1"

if not "%RC%"=="0" (
  echo [ERROR] Завершено с кодом %RC%.
  if defined FROM_EXPLORER (
    echo Нажмите любую клавишу, чтобы закрыть окно...
    pause >nul
  )
)

popd >nul
endlocal & exit /b %RC%

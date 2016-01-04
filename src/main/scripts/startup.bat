@echo off

rem Logmesh Startup Script for Windows

if not "%JRE_HOME%" == "" goto gotJreHome
if not "%JAVA_HOME%" == "" goto gotJavaHome
echo Neither the JAVA_HOME nor the JRE_HOME environment variable is defined
echo At least one of these environment variable is needed to run this program
goto exit

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\javaw.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\jdb.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\javac.exe" goto noJavaHome
set JRE_HOME=%JAVA_HOME%
goto run

:noJavaHome
echo The JAVA_HOME environment variable is not defined correctly.
echo It is needed to run this program in debug mode.
echo NB: JAVA_HOME should point to a JDK not a JRE.
goto exit

:noJreHome
rem Needed at least a JRE
echo The JRE_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto exit

:gotJavaHome
rem No JRE given, use JAVA_HOME as JRE_HOME
set JRE_HOME=%JAVA_HOME%
:gotJreHome
rem Check if we have a usable JRE
if not exist "%JRE_HOME%\bin\java.exe" goto noJreHome
if not exist "%JRE_HOME%\bin\javaw.exe" goto noJreHome
goto run

:run
set TITLE=logmesh
set JAVA_EXE=%JRE_HOME%\bin\java.exe
set JAVA_OPT=-Xms128M -Xmx256M
set EXEC=-jar logmesh.jar
start "%TITLE%" %JAVA_EXE% %JAVA_OPT% %EXEC%

:exit
exit /b 1
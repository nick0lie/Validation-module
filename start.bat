@echo off

REM Запуск docker-compose 
docker-compose up --build -d

REM Запуск фронта
cd frontend
call npm run dev

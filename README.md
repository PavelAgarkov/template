# template — готовый к эксплуатации шаблон сервиса на Go

Шаблон, который можно использовать для разработки новых сервисов на Go без необходимости писать однообразный boilerplate-код.

- предсказуемый и понятный **graceful shutdown** всех сервисов и задач с учетом leader election (если необходимо);
- гибкая архитектура для простого добавления новых модулей и расширения функциональности;
- **watchdog** для мониторинга состояния сервисов и перезапуска при сбоях;
- **leader election** для распределённого управления состоянием между экземплярами;
- быстрый старт нового сервиса на базе готового каркаса.
- readiness barrier - блокировка запросов на чтение до тех пор, пока сервис не будет готов к работе, или другая логика наложения запрета и снятия
- гибкий scheduler с учётом rate-лимитов и врщможности выбрать стратегию остановки задач
- локкер на Redis для распределённой блокировки
- настроенные подключения для PostgreSQL, ClickHouse, Redis
- cron - периодическое выполнение задач
- настроенный логгер для xlog и zap - одинаковый интерфейс для логирования в обоих случаях
- утилиты для recovery паники и мониторинга использования ресурсов

## Как использовать
1. Запустите докер-контейнеры с помощью `docker compose -f dev/docker-compose.yaml up`
2. Запустите миграции базы данных
   create database cloud_template;
   goose -dir migrations postgres "host=0.0.0.0 port=5433 user=postgres password=qwerty12345 dbname=cloud_template sslmode=disable" up
3. Запустите сервисы с помощью `make run_service`
4. В cmd/github.com/PavelAgarkov/template/main.go вы можете изменить настройки сервисов, и заложенный архитектурный стиль.

## Описание директорий
```plaintext
├── api/                 # .proto для gRPC и HTTP API + сгенерированный код
├── application/         # управление жизненным циклом: регистрация и запуск сервисов
├── cmd/                 # исполняемые сервисы, точка входа main.go
├── config/              # конфигурация (подключения к БД и внешним сервисам)
├── container/           # инициализация зависимостей (PostgreSQL, ClickHouse, Redis и т.д.)
├── deploy/              # файлы деплоя для продакшен-окружения
├── dev/                 # docker compose для локальной разработки
├── internal/            # внутренняя логика, бизнес-процессы, обработчики, репозитории
├── migrations/          # миграции БД (Goose)
├── pkg/                 # общие пакеты/утилиты (логирование, клиенты и т.п.)
├── protobuf/            # сгенерированные файлы для gRPC/HTTP API
├── swagger_docs/        # сгенерированная Swagger-документация
└── tools/               # вспомогательные утилиты (генерация кода, миграции и пр.)
```


## Утилиты
- monitor.sh - скрипт для мониторинга использования ресурсов сервисов
- gnuplot - утилита для построения графиков из метрик сервисов
- makefile - содержит команды для сборки, запуска и тестирования сервисов, а также для генерации кода.

## В сервисе реализованы:
- gRPC — взаимодействие между сервисами.
- HTTP (chi, net/http, gorilla/mux) — взаимодействие с внешними клиентами.
- Protobuf — описание структур данных и сервисов.
- PostgreSQL — основное хранилище.
- ClickHouse — аналитика и большие объёмы данных.
- Redis — кэш и координация состояния.
- Goose — миграции.
- CommandBus — асинхронная обработка задач.
- Consumer — потребители сообщений из очередей.
- cron — периодическое выполнение задач.
- Core (planner) — планирование задач и управление состоянием.
- Application — единый жизненный цикл: регистрация/запуск/остановка компонентов.
- Watchdog — мониторинг модулей и перезапуск при сбоях (с Redis-блокировкой).
- Кастомизированный scheduler с учётом rate-лимитов: internal/service/scheduler/scheduler.go.
- tools/protoc — контейнер для генерации protobuf-файлов.
- tools/swagger — генерация Swagger-документации по API.
- monitoring.sh — локальный мониторинг потребления ресурсов.
- gnuplot — построение графиков из метрик локальной нагрузки.
- Деплой в продакшен — директория deploy/.
- Быстрый запуск local-окружения — dev/docker-compose.yaml.

## Установка
```bash
go install github.com/pressly/goose/v3/cmd/goose@v3.23.0 <--- устанавливает goose для миграций
docker build --no-cache -t protoc-go:31.1 -f tools/protoc/Dockerfile .  <---так собирается контейнер для генерации protobuf файлов
```

## Запуск
```bash
docker compose -f dev/docker-compose.yaml up
make build_service # - собирает сервис github.com/PavelAgarkov/template-build и запускает его в фоне
docker compose -f dev/docker-compose.yaml down
```

```bash
go tool pprof -http=127.0.0.1:8071 http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/profile?seconds=30
```

## Миграции
```bash
create database cloud_template;
goose -dir migrations postgres "host=0.0.0.0 port=5433 user=postgres password=qwerty12345 dbname=cloud_template sslmode=disable" up
goose -dir migrations postgres "host=0.0.0.0 port=5433 user=postgres password=qwerty12345 dbname=cloud_template sslmode=disable" down
goose -dir ./migrations create migration_name sql
```

## Полезные команды
```bash
GOPRIVATE=github.com/PavelAgarkov/* go get github.com/PavelAgarkov/service-pkg@3168d74


make proto-gen # - генерирует protobuf файлы
make swagger-gen # - генерирует swagger документацию
make generate_token name=client # - генерирует токен для клиента
```

## Как локально быстро промониторить сервис на использование ресурсов
```bash
make build_service - собирает сервис github.com/PavelAgarkov/template-build и запускает его в фоне
 ./monitor.sh ./cmd/github.com/PavelAgarkov/template/github.com/PavelAgarkov/template-build - пишет в [metrics.csv](metrics.csv), по которым можно построить графики командой
make gnuplot - строит графики из metrics.csv
```

## Технический долг
- Ваши предложения и идеи по улучшению сервиса
- grpc-gateway - добавить поддержку grpc-gateway для преобразования gRPC в HTTP
- Для распределнных блокировок можно рассмотореть etcd, Consul, Zookeeper
- Добавить поддержку других баз данных, таких как MongoDB и др.
- Добавить поддержку других брокеров сообщений, таких как Kafka и др.
- Добавить поддержку других систем кэширования
- Переделать логгер на zap
- Добавить поддержку других систем мониторинга, таких как Prometheus и др.
- Добавить поддержку других систем тестирования, таких как Ginkgo и др.
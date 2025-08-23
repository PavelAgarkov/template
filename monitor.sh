#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <binary-name-or-path>"
  exit 1
fi

target="$1"
BIN_BASENAME="$(basename "$target")"

printed_header=false
csv_file="metrics.csv"
: > "$csv_file"

start_time=$(date +%s)

print_header() {
  printf "%5s %5s %7s %7s %8s %s\n" PID CPU RSS_MB VSZ_MB ELAPSED CMD
}

# Находим PID по argv[0] (первому аргументу процесса), сравниваем basename
find_pid() {
  local pid argv0 base0
  for d in /proc/[0-9]*; do
    pid="${d#/proc/}"
    # пропускаем текущую шелл-сессию и её родителей
    [[ "$pid" == "$$" || "$pid" == "$PPID" || "$pid" == "$BASHPID" ]] && continue
    # читаем первый аргумент (до первого NUL) — это и есть исполняемый файл
    if IFS= read -r -d $'\0' argv0 < "$d/cmdline" 2>/dev/null; then
      base0="$(basename "$argv0")"
      if [[ "$base0" == "$BIN_BASENAME" ]]; then
        echo "$pid"
      fi
    fi
  done | tail -n1
}

while :; do
  pid="$(find_pid || true)"

  if [[ -t 1 ]]; then
    clear
    print_header
  elif ! $printed_header; then
    print_header
    printed_header=true
  fi

  if [[ -z "${pid:-}" ]]; then
    echo "$target not running"
  else
    # command= чтобы получить ВЕСЬ командлайн
    ps -p "$pid" -o pid=,pcpu=,rss=,vsz=,etime=,command= |
      awk -v csv="$csv_file" -v start="$start_time" '{
        rss_mb=$3/1024; vsz_mb=$4/1024;
        # вся команда (включая пробелы): начинается с 6-го поля
        cmd = substr($0, index($0,$6))
        printf "%5s %5s %7.2f %7.2f %8s %s\n", $1,$2,rss_mb,vsz_mb,$5,cmd
        rel = systime() - start
        printf "%s,%s,%.2f,%.2f,%s,%d\n", $1,$2,rss_mb,vsz_mb,$5,rel >> csv
        fflush(csv)  # немедленно пишем на диск
      }'
  fi

  sleep 1
done

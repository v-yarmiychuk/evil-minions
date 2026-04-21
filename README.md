## evil-minions

Инструмент нагрузочного тестирования [Salt](https://github.com/saltstack/salt)
в связке с [Uyuni](https://www.uyuni-project.org/) и [SUSE
Manager](https://www.suse.com/products/suse-manager/).

### Назначение

Подменяется точка входа `salt-minion`: один реальный minion остаётся эталоном,
дополнительно поднимается заданное число логических minion с префиксом id (по
умолчанию `evil-*`). Для master они выглядят как отдельные узлы. Ответы на
команды вне встроенных исключений воспроизводятся по заранее снятому с эталона
трафику (baseline).

### Форк от upstream

База: `uyuni-project/evil-minions`. Изменения в этом репозитории:

- onedir-окружение Salt, основная проверка на 3007.x;
- совместимость callback-перехвата `AsyncPubChannel.on_recv` между Salt 3006.x и 3007.x;
- совместимость с Python 3.13 (отказ от `distutils`);
- перехват через актуальный `salt.channel.client`;
- доработки асинхронных callback;
- таргетинг `glob` для шаблонов вида `evil-*`;
- один ZMQ PUSH на процесс при пересылке событий в прокси (вместо сокета на сообщение);
- `--log-level`, уровень наследуется в дочерних процессах (`EVIL_MINIONS_LOG_LEVEL`);
- число процессов Hydra: из `--processes` либо авто от `--count` и `cpu_count()`;
- интервал ожидания baseline в `mimic()`: из `--mimic-poll` либо авто от `--count`;
- корректный выход из `mimic()` при стартовом вызове с `fun is None`.

Upstream без этих изменений: https://github.com/uyuni-project/evil-minions.

### Установка

```bash
cd /opt/saltstack/
git clone https://github.com/v-yarmiychuk/evil-minions.git
cd evil-minions
```

### systemd

```bash
# Из директории evil-minions
sudo cp systemd/evil-minions.service /etc/systemd/system/
sudo cp systemd/evil-minions.env /etc/
sudo systemctl daemon-reload
sudo systemctl disable --now salt-minion
sudo systemctl enable --now evil-minions
```

Основные параметры службы могут быть изменены в файле `/etc/evil-minions.env`.
После изменения файла перезапустите службу:

```bash
sudo systemctl restart evil-minions
```

### Параметры запуска

| Параметр | Назначение |
|----------|------------|
| `--count` | Число симулируемых minion (дефолт скрипта: 100). |
| `--ramp-up-delay` | Задержка между стартами соседних голов, сек. |
замедления относительно записанных интервалов). |
| `--processes` | Число процессов Hydra; иначе авто от `--count` и CPU. |
| `--log-level` | `DEBUG` … `CRITICAL`, дефолт `INFO`. |

По умолчанию PKI evil-minions хранится в постоянном каталоге
`/var/lib/evil-minions/pki/<minion_id>`. При необходимости базовый путь можно
переопределить переменной окружения `EVIL_MINIONS_PKI_BASE`.

Дедупликация `_return` включена по умолчанию и работает по паре `(minion_id, jid)`.
Если `jid` пустой (`None`/`''`), дедупликация не применяется.
Параметры через env: `EVIL_MINIONS_DEDUP_TTL_SEC` (по умолчанию `180`), `EVIL_MINIONS_DEDUP_MAX` (по умолчанию `30000`).

Для grains профилей поле `master` из `data/grains.json` игнорируется: при старте
оно принудительно берётся из grains реального minion.

При старте каждой головы во все основные сетевые поля grains подставляется **реальный
исходящий IPv4** к мастеру (тот же адрес, что видит мастер на TCP-сокете). Так
совпадает кэш presence на мастере с адресами из `grains.items`, если в профиле
были «чужие» IP из снимка. Отключить: `EVIL_MINIONS_REAL_IP_OVERLAY=0` (или `false`/`no`).

### Проверка

```bash
salt '*' test.ping
```

Команда без baseline у эталонного minion: ответ с ошибкой до первого успешного
выполнения на реальном minion с тем же `fun`/аргументами.

### Troubleshooting: ключи и регистрация

#### Симптомы

- В логах master: `Authentication attempt from <minion_id> failed, the public
  keys did not match`.
- В `salt-key -L` один и тот же minion может оказаться в конфликтных состояниях
  (например, после ручной чистки PKI на клиенте и старых ключей на мастере).
- Синхронные `salt ...` иногда дают timeout под нагрузкой event bus, при этом
  async-джобы могут успешно возвращаться.

#### Важно понимать

- При **обычном restart** `salt-minion` ключи evil-minions не обязаны меняться, если
  сохраняется каталог `EVIL_MINIONS_PKI_BASE`.
- Проблема обычно появляется после потери/очистки локального PKI у evil-minions
  или рассинхрона key-state на мастере.

#### Сценарий восстановления key-state

1) На evil-host (где запущен Evil Minions):

```bash
sudo systemctl stop evil-minions
# Чистить PKI только если действительно нужен полный ресинк ключей:
sudo rm -rf /var/lib/evil-minions/pki/*
```

2) На Salt Master (в контейнере, если master контейнеризирован) **[ БУДУТ
УДАЛЕНЫ ВСЕ КЛЮЧИ! ]**:

```bash
sudo salt-key -D -y
```

3) На хосте Evil Minions:

```bash
sudo systemctl start evil-minions
```

4) На хосте Salt Master:

```bash
sudo salt-key -A -y
```

3) Проверка на хосте Salt Master:

```bash
sudo salt '*' test.ping --async
sudo salt-run jobs.lookup_jid <JID>
```

### Ограничения

- Транспорт: ZeroMQ.
- Таргетинг: `glob`, список id, точное совпадение id; compound и прочее — не заявлено.
- Неполная эмуляция: `mine`, `beacon`, часть сценариев `state.sls` / concurrency.
- Uyuni: без Action Chains и ряда специфичных функций.
- Масштаб на одном хосте: рост `--count` линейно увеличивает число полноценных
  клиентских сессий (сеть, CPU, крипта). Для больших значений — несколько узлов
  или снижение `--count`.

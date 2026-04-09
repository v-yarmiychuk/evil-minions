## evil-minions

`evil-minions` — генератор нагрузки для [Salt](https://github.com/saltstack/salt).
Он используется для тестирования масштабируемости Salt, [Uyuni](https://www.uyuni-project.org/)
и [SUSE Manager](https://www.suse.com/products/suse-manager/).

### Отличия этого форка

Этот репозиторий является форком `uyuni-project/evil-minions` и содержит
практические доработки для современных окружений:

- совместимость с Salt `3007.x` (onedir-пакеты);
- запуск через встроенный Python Salt (`/opt/saltstack/salt/bin/python3.10`);
- исправления совместимости с Python 3.13 (`distutils` -> `shutil.which`);
- адаптация перехвата транспорта к актуальному API (`salt.channel.client`);
- исправления обработки событий и callback в асинхронном режиме;
- поддержка `glob`-таргетинга вида `evil-*` (кроме точных id и `*`);
- обновлённые инструкции по установке и запуску для Debian/Ubuntu.

Если вам нужна максимально близкая к upstream-версии логика без этих правок,
используйте исходный репозиторий `uyuni-project/evil-minions`.

### Что это

`evil-minions` подменяет запуск `salt-minion` и поднимает рядом набор
симулированных minion-узлов ("evil" minions).

Эти minion-узлы:

- копируют поведение реального minion;
- отвечают мастеру как отдельные узлы (с разными id);
- работают достаточно легко, чтобы запускать сотни и тысячи экземпляров.

### Установка

#### SUSE (RPM)

```bash
# при необходимости замените openSUSE_Leap_15.0 на нужный дистрибутив
zypper addrepo https://download.opensuse.org/repositories/systemsmanagement:/sumaform:/tools/openSUSE_Leap_15.0/systemsmanagement:sumaform:tools.repo
zypper install evil-minions
```

#### Debian/Ubuntu и другие дистрибутивы (из исходников)

```bash
git clone https://github.com/moio/evil-minions.git
cd evil-minions

# пример зависимостей для Debian (без system-wide pip, совместимо с PEP 668)
sudo apt-get install -y python3-msgpack python3-zmq python3-tornado
```

### Настройка systemd

Создайте drop-in для `salt-minion` и используйте `override.conf` из репозитория:

```bash
sudo mkdir -p /etc/systemd/system/salt-minion.service.d
sudo cp override.conf /etc/systemd/system/salt-minion.service.d/override.conf
sudo systemctl daemon-reload
sudo systemctl restart salt-minion
```

Для Salt onedir (например, `/opt/saltstack/salt`) рекомендуется запуск через
встроенный Python и рабочую директорию проекта:

```ini
[Service]
ExecStart=
ExecStart=/opt/saltstack/salt/bin/python3.10 /home/user/evil-minions/evil-minions --count=10 --ramp-up-delay=0 --slowdown-factor=0.0
WorkingDirectory=/home/user/evil-minions
```

### Использование

После запуска `salt-minion` автоматически поднимаются evil minions
(`--count=10` по умолчанию).

Базовая проверка:

```bash
salt '*' test.ping
salt 'evil-*' test.ping
```

Если evil minion получил незнакомую команду, он ждёт, пока реальный minion
сначала выполнит её и "обучит" симуляцию.

Параметры запуска задаются через `ExecStart` в файле:

`/etc/systemd/system/salt-minion.service.d/override.conf`

### Основные параметры

#### `--count`

Количество симулированных minion-узлов.

#### `--ramp-up-delay`

Задержка (в секундах) между запуском соседних evil minions.
Полезно для плавного старта при большой нагрузке.

#### `--slowdown-factor`

Коэффициент замедления ответов:

- `0.0` — максимально быстро;
- `1.0` — примерно как исходный minion;
- `2.0` — в 2 раза медленнее;
- `0.5` — в 2 раза быстрее.

#### `--random-slowdown-factor`

Добавляет случайный разброс к `slowdown-factor`.
Например, при `slowdown-factor=1.0` и `random-slowdown-factor=0.2`
каждый evil minion получит постоянный коэффициент в диапазоне `1.0..1.2`.

### Ограничения

- поддерживается только транспорт ZeroMQ;
- таргетинг minion поддерживает `glob` (например, `evil-*`) и точные id;
- часть возможностей Salt воспроизводится не полностью (`mine`, `beacon`,
  `state.sls` с `concurrent`);
- часть возможностей Uyuni не поддерживается (например, Action Chains).

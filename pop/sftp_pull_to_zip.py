import argparse
import json
import stat
import time
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import List, Tuple, Iterator, Optional

import paramiko
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED

try:
    import yaml  # опционально: pip install pyyaml
    HAVE_YAML = True
except ImportError:
    HAVE_YAML = False


# ----------------------------- config -----------------------------

def load_config(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        return {}
    if cfg_path.suffix.lower() in (".yaml", ".yml"):
        if not HAVE_YAML:
            raise RuntimeError("Для YAML-конфига установите pyyaml или используйте JSON.")
        with cfg_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    if cfg_path.suffix.lower() == ".json":
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f) or {}
    return {}


# ----------------------------- sftp walk -----------------------------

def sftp_walk(sftp: paramiko.SFTPClient, top: str) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Аналог os.walk для SFTP. Возвращает (dirpath, dirnames, filenames).
    Симлинки на каталоги не разворачиваем (как followlinks=False).
    """
    dirs, files = [], []
    try:
        for e in sftp.listdir_attr(top):
            name = e.filename
            mode = e.st_mode
            if stat.S_ISDIR(mode):
                if name not in (".", ".."):
                    dirs.append(name)
            elif stat.S_ISREG(mode):
                files.append(name)
            else:
                if stat.S_ISLNK(mode):
                    # мягкая попытка: если ссылка указывает на файл — считаем файлом; если на каталог — как каталог
                    try:
                        st = sftp.stat(str(PurePosixPath(top) / name))
                        if stat.S_ISREG(st.st_mode):
                            files.append(name)
                        elif stat.S_ISDIR(st.st_mode):
                            dirs.append(name)
                    except Exception:
                        pass
        yield top, sorted(dirs), sorted(files)
        for d in dirs:
            new_path = str(PurePosixPath(top) / d)
            yield from sftp_walk(sftp, new_path)
    except FileNotFoundError:
        return


# ----------------------------- helpers -----------------------------

def posix_to_zip_datetime(epoch_secs: float) -> tuple:
    """UNIX time -> (Y,M,D,H,M,S) для ZipInfo (год не ниже 1980)."""
    t = time.localtime(epoch_secs)
    return (max(t.tm_year, 1980), t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)


def add_file_to_zip_from_sftp(
    sftp: paramiko.SFTPClient,
    zipf: ZipFile,
    remote_path: str,
    arcname: str
):
    """Читает файл по SFTP и пишет напрямую в ZIP (потоково), сохраняя mtime и права."""
    st = sftp.stat(remote_path)
    zi = ZipInfo(arcname)
    zi.compress_type = ZIP_DEFLATED
    zi.date_time = posix_to_zip_datetime(st.st_mtime)
    zi.external_attr = (st.st_mode & 0xFFFF) << 16  # Unix-права для распаковки на Unix
    with sftp.open(remote_path, "rb") as fsrc, zipf.open(zi, "w") as fdst:
        while True:
            chunk = fsrc.read(1024 * 1024)
            if not chunk:
                break
            fdst.write(chunk)


def compute_output_zip_path(out_dir_arg: Optional[str]) -> Path:
    """
    out_dir_arg — путь к ДИРЕКТОРИИ вывода (как в конфиге/CLI).
    Если передали путь с .zip — берём его родителя.
    Если не задано — используем ./archives.
    Имя файла: YYYY-MM-DD.zip по локальной дате.
    """
    if out_dir_arg:
        p = Path(out_dir_arg).expanduser().resolve()
        if p.suffix.lower() == ".zip":
            p = p.parent
    else:
        p = Path.cwd() / "archives"
    p.mkdir(parents=True, exist_ok=True)
    fname = f"bl-backup-{date.today():%Y-%m-%d}.zip"
    return p / fname


def run_pg_dump_into_zip(
    ssh: paramiko.SSHClient,
    zipf: ZipFile,
    pg_cfg: dict,
    verbose: bool = False
):
    """
    Запускает pg_dump на VM и пишет его stdout в файл внутри ZIP.
    pg_cfg:
      enabled: bool
      db_host, db_port, db_name, db_user, db_password (optional), pg_dump_path (default 'pg_dump'), extra_args (list)
    """
    if not pg_cfg or not pg_cfg.get("enabled"):
        return

    pg_dump_path = pg_cfg.get("pg_dump_path", "pg_dump")
    db_host = pg_cfg.get("db_host", "127.0.0.1")
    db_port = int(pg_cfg.get("db_port", 5432))
    db_name = pg_cfg.get("db_name")
    db_user = pg_cfg.get("db_user")
    db_password = pg_cfg.get("db_password", None)
    extra_args = pg_cfg.get("extra_args", [])

    if not db_name or not db_user:
        raise ValueError("pg_dump.enabled=true, но не указаны db_name или db_user")

    # Сформируем команду pg_dump (plain SQL)
    # ВАЖНО: паролем безопаснее пользоваться через .pgpass на VM.
    # Если всё же нужно передать пароль — используем env PGPASSWORD только внутри команды.
    env_prefix = ""
    if db_password:
        # Обернём в один bash -lc, чтобы переменная окружения применялась только к этому процессу
        env_prefix = f"PGPASSWORD='{db_password}' "

    # Собираем аргументы
    args = [
        pg_dump_path,
        "-h", str(db_host),
        "-p", str(db_port),
        "-U", str(db_user),
        "-F", "p",   # plain SQL
        "--encoding", "UTF8",
        str(db_name),
    ]
    # Вставим дополнительные опции, если есть
    if isinstance(extra_args, list):
        args[1:1] = extra_args  # разрешаем добавлять в начало до -h

    # Сформируем единую строку (экранируя аргументы для bash)
    def shquote(x: str) -> str:
        return x

    cmd = env_prefix + " ".join(shquote(a) for a in args)

    # Запуск в bash для подхвата PATH и .pgpass
    print(cmd)
    full_cmd = shquote(cmd)
    if verbose:
        print(f"[INFO] pg_dump cmd on VM: {full_cmd}")

    # Выполним команду и прочитаем stdout потоково
    stdin, stdout, stderr = ssh.exec_command(full_cmd, get_pty=False)

    # Создадим файл в ZIP с именем db-<name>-YYYYmmdd_HHMMSS.sql
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_name = f"db-{db_name}-{ts}.sql"
    zi = ZipInfo(sql_name)
    zi.compress_type = ZIP_DEFLATED
    zi.date_time = posix_to_zip_datetime(time.time())
    with zipf.open(zi, "w") as zdst:
        while True:
            chunk = stdout.channel.recv(1024 * 1024)
            if not chunk:
                if stdout.channel.exit_status_ready():
                    break
                # если канал не готов — небольшая пауза не обязательна, paramiko сам блокирует
                continue
            zdst.write(chunk)

    # Проверим код возврата
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        # вытащим stderr для диагностики
        err = stderr.read().decode("utf-8", "ignore")
        raise RuntimeError(f"pg_dump завершился с кодом {exit_status}. stderr:\n{err}")

    if verbose:
        print(f"[INFO] Добавлен SQL-дамп в ZIP: {sql_name}")


# ----------------------------- main -----------------------------

from pathlib import Path
import ctypes
from ctypes import wintypes
import uuid
from datetime import date
import os
import sys

# --- функция получения папки "Документы" в Windows ---
def get_windows_documents_dir() -> Path:
    """
    Возвращает Path к папке "Документы" текущего пользователя в Windows,
    используя SHGetKnownFolderPath. Если не удалось, пытается USERPROFILE\\Documents.
    """
    try:
        # GUID FOLDERID_Documents = {FDD39AD0-238F-46AF-ADB4-6C85480369C7}
        guid = uuid.UUID("FDD39AD0-238F-46AF-ADB4-6C85480369C7")

        class GUID(ctypes.Structure):
            _fields_ = [
                ("Data1", ctypes.c_ulong),
                ("Data2", ctypes.c_ushort),
                ("Data3", ctypes.c_ushort),
                ("Data4", ctypes.c_ubyte * 8)
            ]

        data4 = (ctypes.c_ubyte * 8).from_buffer_copy(guid.bytes[8:])
        guid_struct = GUID(guid.time_low, guid.time_mid, guid.time_hi_version, data4)

        SHGetKnownFolderPath = ctypes.windll.shell32.SHGetKnownFolderPath
        SHGetKnownFolderPath.argtypes = [ctypes.POINTER(GUID), wintypes.DWORD, wintypes.HANDLE, ctypes.POINTER(ctypes.c_wchar_p)]
        SHGetKnownFolderPath.restype = wintypes.HRESULT

        ppath = ctypes.c_wchar_p()
        res = SHGetKnownFolderPath(ctypes.byref(guid_struct), 0, 0, ctypes.byref(ppath))
        if res == 0 and ppath.value:
            path = Path(ppath.value)
            # освободим указатель (CoTaskMemFree)
            ctypes.windll.ole32.CoTaskMemFree(ppath)
            try:
                path.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            return path
    except Exception:
        # пробуем fallback
        pass

    # fallback: %USERPROFILE%\Documents
    up = os.environ.get("USERPROFILE") or os.environ.get("HOME")
    if up:
        cand = Path(up) / "Documents"
        try:
            cand.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        return cand

    # очень крайний fallback
    return Path.home()

# --- функция для уникального имени архива ---
def make_unique_archive_path(base_dir: Path, base_name: str) -> Path:
    """
    base_name без расширения; возвращает уникальный путь base_name.zip,
    при совпадении base_name-1.zip, base_name-2.zip и т.д.
    """
    base_dir = Path(base_dir)
    ext = ".zip"
    candidate = base_dir / (base_name + ext)
    if not candidate.exists():
        return candidate
    # добавляем -1, -2...
    i = 1
    while True:
        candidate = base_dir / f"{base_name}-{i}{ext}"
        if not candidate.exists():
            return candidate
        i += 1

def main():
    # 1) загрузим config рядом со скриптом, если есть
    default_cfg = {}
    for name in ("config.yaml", "config.yml", "config.json"):
        cfg_path = Path(__file__).with_name(name)
        if cfg_path.exists():
            default_cfg = load_config(cfg_path)
            print(f"[INFO] Загружен конфиг {cfg_path}")
            break

    # 2) CLI с дефолтами из конфига
    ap = argparse.ArgumentParser(
        description="Скачать файлы с Linux-VM в ZIP (YYYY-MM-DD.zip), затем сделать pg_dump на VM и положить SQL в ZIP."
    )
    ap.add_argument("--host", default=default_cfg.get("host"))
    ap.add_argument("--port", type=int, default=default_cfg.get("port", 22))
    ap.add_argument("--user", default=default_cfg.get("user"))

    auth = ap.add_mutually_exclusive_group()
    auth.add_argument("--keyfile", default=default_cfg.get("keyfile"))
    auth.add_argument("--password", default=default_cfg.get("password"))

    ap.add_argument("--remote-dir", default=default_cfg.get("remote_dir"))
    ap.add_argument("--out", default=default_cfg.get("out"))  # ДИРЕКТОРИЯ вывода
    ap.add_argument("--exclude", action="append", default=default_cfg.get("exclude", []),
                    help="Простые окончания имён для исключения (endswith), можно несколько раз.")
    ap.add_argument("--verbose", action="store_true", default=bool(default_cfg.get("verbose", False)))

    # Можно переопределять поля pg_dump из CLI (необязательно)
    ap.add_argument("--pg-enabled", action="store_true",
                    default=bool(default_cfg.get("pg_dump", {}).get("enabled", False)))
    ap.add_argument("--pg-host", default=default_cfg.get("pg_dump", {}).get("db_host"))
    ap.add_argument("--pg-port", type=int, default=default_cfg.get("pg_dump", {}).get("db_port", 5432))
    ap.add_argument("--pg-name", default=default_cfg.get("pg_dump", {}).get("db_name"))
    ap.add_argument("--pg-user", default=default_cfg.get("pg_dump", {}).get("db_user"))
    ap.add_argument("--pg-pass", default=default_cfg.get("pg_dump", {}).get("db_password"))
    ap.add_argument("--pg-dump-path", default=default_cfg.get("pg_dump", {}).get("pg_dump_path", "pg_dump"))

    args = ap.parse_args()

    if sys.platform.startswith("win") and not args.out:
        docs = get_windows_documents_dir()
        base_name = date.today().strftime("%Y-%m-%d")
        dest = make_unique_archive_path(docs, f"bl-backup-{base_name}")
        args.out = str(dest)
        print(f"[INFO] --archive не задан, сохраню архив в: {args.out}")

    # обязательные поля
    if not args.host or not args.user or not args.remote_dir:
        ap.error("Нужно указать --host, --user, --remote-dir (или задать их в config.*)")

    out_zip = compute_output_zip_path(args.out)

    # 3) подключаемся по SSH/SFTP
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        if args.keyfile:
            pkey = None
            key_path = Path(args.keyfile).expanduser()
            for loader in (paramiko.ECDSAKey, paramiko.Ed25519Key, paramiko.RSAKey, paramiko.DSSKey):
                try:
                    pkey = loader.from_private_key_file(str(key_path))
                    break
                except Exception:
                    continue
            if pkey is None:
                raise ValueError(f"Не удалось прочитать ключ: {key_path}")
            client.connect(hostname=args.host, port=args.port, username=args.user, pkey=pkey, timeout=30)
        else:
            client.connect(hostname=args.host, port=args.port, username=args.user, password=args.password, timeout=30)

        sftp = client.open_sftp()
        remote_root = sftp.normalize(args.remote_dir)
        if args.verbose:
            print(f"[INFO] Remote root: {remote_root}")
            print(f"[INFO] Output ZIP:  {out_zip}")

        files_copied = 0
        bytes_copied = 0

        with ZipFile(out_zip, mode="w", compression=ZIP_DEFLATED, allowZip64=True) as zipf:
            # 4) файлы по SFTP -> ZIP
            for dirpath, dirnames, filenames in sftp_walk(sftp, remote_root):
                rel_dir = str(PurePosixPath(dirpath).relative_to(remote_root))
                for name in filenames:
                    remote_file = str(PurePosixPath(dirpath) / name)
                    if args.exclude and any(remote_file.endswith(suf) for suf in args.exclude):
                        if args.verbose:
                            print(f"[SKIP] {remote_file}")
                        continue
                    arcname = f"{rel_dir}/{name}" if rel_dir else name
                    if args.verbose:
                        print(f"[GET ] {remote_file} -> {arcname}")
                    size = sftp.stat(remote_file).st_size
                    add_file_to_zip_from_sftp(sftp, zipf, remote_file, arcname)
                    files_copied += 1
                    bytes_copied += size

            # 5) опционально: pg_dump -> ZIP
            if args.pg_enabled:
                pg_cfg = {
                    "enabled": True,
                    "db_host": args.pg_host or default_cfg.get("pg_dump", {}).get("db_host", "127.0.0.1"),
                    "db_port": args.pg_port,
                    "db_name": args.pg_name,
                    "db_user": args.pg_user,
                    "db_password": args.pg_pass,
                    "pg_dump_path": args.pg_dump_path or default_cfg.get("pg_dump", {}).get("pg_dump_path", "pg_dump"),
                    "extra_args": default_cfg.get("pg_dump", {}).get("extra_args", []),
                }
                if args.verbose:
                    print(f"[INFO] Выполняю pg_dump для БД '{pg_cfg['db_name']}' на VM...")
                run_pg_dump_into_zip(client, zipf, pg_cfg, verbose=args.verbose)

        if args.verbose:
            print(f"[DONE] Files: {files_copied}, Size: {bytes_copied/1024/1024:.2f} MiB")
        print(f"[ГОТОВО] ZIP создан: {out_zip}")

        sftp.close()
    except Exception as e:
        print(f"[ОШИБКА] {e}")
        raise
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

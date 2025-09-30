import argparse
import json
import os
import stat
import time
from datetime import date
from pathlib import Path, PurePosixPath
from typing import List, Tuple, Iterator

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
    if cfg_path.suffix.lower() in [".yaml", ".yml"]:
        if not HAVE_YAML:
            raise RuntimeError("Для YAML-конфига установите pyyaml или используйте JSON.")
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    if cfg_path.suffix.lower() == ".json":
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    return {}


# ----------------------------- sftp walk -----------------------------

def sftp_walk(sftp: paramiko.SFTPClient, top: str) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Аналог os.walk для SFTP. Возвращает (dirpath, dirnames, filenames).
    Ссылки на каталоги не разворачиваем.
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
                # мягкая попытка следовать симлинку: если он указывает на файл — добавим как файл,
                # если на каталог — добавим как каталог (без follow глубже на этом шаге)
                if stat.S_ISLNK(mode):
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
    """Преобразует UNIX-время в кортеж (Y,M,D,H,M,S) для ZipInfo (год не ниже 1980)."""
    t = time.localtime(epoch_secs)
    return (max(t.tm_year, 1980), t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)


def add_file_to_zip_from_sftp(
    sftp: paramiko.SFTPClient,
    zipf: ZipFile,
    remote_path: str,
    arcname: str
):
    """Читает файл по SFTP и пишет его напрямую в ZIP (потоково), сохраняя mtime и права."""
    st = sftp.stat(remote_path)

    zi = ZipInfo(arcname)
    zi.compress_type = ZIP_DEFLATED
    zi.date_time = posix_to_zip_datetime(st.st_mtime)
    # сохранить Unix-права во внешних атрибутах ZIP (будут видны на Unix при распаковке)
    zi.external_attr = (st.st_mode & 0xFFFF) << 16

    # потоковая запись без буферизации всего файла в памяти
    with sftp.open(remote_path, "rb") as fsrc, zipf.open(zi, "w") as fdst:
        while True:
            chunk = fsrc.read(1024 * 1024)
            if not chunk:
                break
            fdst.write(chunk)


def compute_output_zip_path(out_dir_arg: str | None) -> Path:
    """
    out_dir_arg — путь к ДИРЕКТОРИИ вывода (как в конфиге/CLI).
    Если передали путь с .zip — берём его родителя.
    Если не задано — используем ./archives.
    Имя файла: YYYY-MM-DD.zip по локальной дате.
    """
    if out_dir_arg:
        p = Path(out_dir_arg).expanduser().resolve()
        if p.suffix.lower() == ".zip":
            p = p.parent  # на случай, если ошибочно указали файл
    else:
        p = Path.cwd() / "archives"

    p.mkdir(parents=True, exist_ok=True)
    fname = f"bl-backup-{date.today():%Y-%m-%d}.zip"
    return p / fname


# ----------------------------- main -----------------------------

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
        description="Скачать все файлы с Linux-VM по SFTP и упаковать в ZIP. out — директория, имя = YYYY-MM-DD.zip"
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
    args = ap.parse_args()

    # валидация обязательных
    if not args.host or not args.user or not args.remote_dir:
        ap.error("Нужно указать --host, --user, --remote-dir (либо задать их в config.*)")
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
            print(f"[INFO] Output ZIP: {out_zip}")

        # 4) обходим дерево и пишем сразу в ZIP
        files_copied = 0
        bytes_copied = 0

        with ZipFile(out_zip, mode="w", compression=ZIP_DEFLATED, allowZip64=True) as zipf:
            for dirpath, dirnames, filenames in sftp_walk(sftp, remote_root):
                rel_dir = str(PurePosixPath(dirpath).relative_to(remote_root))
                # добавлять пустые каталоги в ZIP не обязательно, пропустим

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

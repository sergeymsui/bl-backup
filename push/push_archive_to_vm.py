import argparse
import json
import os
import stat
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple

import paramiko

try:
    import yaml  # pip install pyyaml (опционально)
    HAVE_YAML = True
except Exception:
    HAVE_YAML = False


# ----------------------------- utils -----------------------------

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

def normalize_arcpath(p: str) -> str:
    # В ZIP могут встретиться обратные слэши; приводим к POSIX
    p = p.replace("\\", "/")
    # Убираем лидирующие / и ./ чтобы не делать абсолютные пути на сервере
    while p.startswith("/") or p.startswith("./"):
        p = p[1:] if p.startswith("/") else p[2:]
    return p

def is_safe_join(root: PurePosixPath, rel: PurePosixPath) -> bool:
    # Защита от path traversal: убедимся, что итоговый путь остаётся внутри root
    try:
        return str(root.joinpath(rel)).startswith(str(root))
    except Exception:
        return False

def ensure_remote_dirs(sftp: paramiko.SFTPClient, path: str, verbose=False):
    # Создаёт все директории по пути (как mkdir -p)
    parts = PurePosixPath(path).parts
    cur = PurePosixPath("/")
    # Если путь относительный — стартуем от пустого (не корня)
    if not path.startswith("/"):
        cur = PurePosixPath(".")
    for part in parts:
        cur = cur / part
        sp = str(cur)
        try:
            sftp.mkdir(sp)
            if verbose:
                print(f"[MKDIR] {sp}")
        except IOError:
            # уже существует — ок
            pass

def set_times_and_mode(sftp: paramiko.SFTPClient, rpath: str,
                       mtime: Optional[int], mode: Optional[int], verbose=False):
    # mtime/atime
    try:
        if mtime is not None:
            # atime == mtime, если отдельного нет
            sftp.utime(rpath, (mtime, mtime))
    except Exception:
        if verbose:
            print(f"[WARN] utime failed for {rpath}")
    # права
    try:
        if mode is not None:
            sftp.chmod(rpath, mode & 0o7777)
    except Exception:
        if verbose:
            print(f"[WARN] chmod failed for {rpath}")

def detect_zip_unix_mode(zi: zipfile.ZipInfo) -> Tuple[Optional[int], bool]:
    """
    Возвращает (mode, is_symlink) из внешних атрибутов ZipInfo (если были сохранены на Unix).
    """
    ext = zi.external_attr >> 16
    mode = ext & 0o7777 if ext else None
    # Тип файла в верхних битах:
    is_symlink = (ext & 0o170000) == stat.S_IFLNK
    return mode, is_symlink

def first_top_level(zip_or_tar_iterable) -> Optional[str]:
    """
    Пытается определить единственный верхний каталог в архиве.
    Возвращает его имя (без хвоста /), либо None.
    """
    tops = set()
    for name in zip_or_tar_iterable:
        top = normalize_arcpath(name).split("/", 1)[0]
        if top:
            tops.add(top)
        if len(tops) > 1:
            return None
    return next(iter(tops)) if tops else None


# ----------------------------- uploaders -----------------------------

def upload_zip(sftp: paramiko.SFTPClient, zip_path: Path, remote_root: str,
               strip_top_level: bool, verbose=False):
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Список имён для определения общего верхнего каталога
        top = first_top_level([i.filename for i in zf.infolist()])
        for zi in zf.infolist():
            name = normalize_arcpath(zi.filename)
            if not name or name.endswith("/"):
                # каталог (или пустая строка) — создадим директорию и дальше
                if not name:
                    continue
                rel = name[:-1]
                if strip_top_level and top and rel.startswith(top + "/"):
                    rel = rel[len(top) + 1:]
                rdir = str(PurePosixPath(remote_root) / rel) if rel else remote_root
                ensure_remote_dirs(sftp, rdir, verbose=verbose)
                # выставим права директории, если есть
                mode, _ = detect_zip_unix_mode(zi)
                set_times_and_mode(sftp, rdir, None, mode, verbose=verbose)
                continue

            # файл или, возможно, symlink
            rel = name
            if strip_top_level and top and rel.startswith(top + "/"):
                rel = rel[len(top) + 1:]
            if not rel:
                continue

            rpath = str(PurePosixPath(remote_root) / rel)
            # защита от traversal
            if not is_safe_join(PurePosixPath(remote_root), PurePosixPath(rel)):
                if verbose:
                    print(f"[SKIP] unsafe path: {rel}")
                continue

            # Убедимся, что родительская директория есть
            ensure_remote_dirs(sftp, str(PurePosixPath(rpath).parent), verbose=verbose)

            mode, is_link = detect_zip_unix_mode(zi)

            # Попытка обработать symlink (если архив создан на Unix и атрибуты сохранены)
            if is_link:
                try:
                    # В ZIP содержимое symlink — это целевая строка ссылки
                    link_target = zf.read(zi).decode("utf-8", "surrogateescape")
                    try:
                        sftp.remove(rpath)
                    except Exception:
                        pass
                    sftp.symlink(link_target, rpath)
                    if verbose:
                        print(f"[LINK] {rpath} -> {link_target}")
                    # Права на симлинк обычно не применяются
                    return
                except Exception:
                    # если не вышло — упадём обратно к записи файла
                    pass

            # Обычный файл: выгружаем потоково
            with zf.open(zi, "r") as src, sftp.open(rpath, "wb") as dst:
                if verbose:
                    print(f"[PUT ] {rpath}")
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

            # Метаданные (mtime из date_time; права из external_attr)
            # date_time -> (Y,M,D,H,M,S), переведём в epoch (без TZ)
            try:
                dt_tuple = zi.date_time
                # Примем локальное время как naive (zip не хранит TZ) — это нормально для большинства кейсов
                import time as _time
                import datetime as _dt
                mtime = int(_time.mktime(_dt.datetime(*dt_tuple).timetuple()))
            except Exception:
                mtime = None
            set_times_and_mode(sftp, rpath, mtime, mode, verbose=verbose)


def upload_tar(sftp: paramiko.SFTPClient, tar_path: Path, remote_root: str,
               strip_top_level: bool, verbose=False):
    mode_map = {
        tarfile.SYMTYPE: "symlink",
        tarfile.DIRTYPE: "dir",
        tarfile.AREGTYPE: "file",
        tarfile.REGTYPE: "file",
        tarfile.LNKTYPE: "hardlink",
    }
    with tarfile.open(tar_path, "r:*") as tf:
        # Определим общий топ
        top = first_top_level([m.name for m in tf.getmembers()])
        for m in tf:
            name = normalize_arcpath(m.name)
            if not name:
                continue
            rel = name
            if strip_top_level and top and rel.startswith(top + "/"):
                rel = rel[len(top) + 1:]
            if not rel:
                continue

            rpath = str(PurePosixPath(remote_root) / rel)
            if not is_safe_join(PurePosixPath(remote_root), PurePosixPath(rel)):
                if verbose:
                    print(f"[SKIP] unsafe path: {rel}")
                continue

            kind = mode_map.get(m.type, "other")

            if kind == "dir":
                ensure_remote_dirs(sftp, rpath, verbose=verbose)
                set_times_and_mode(sftp, rpath, int(m.mtime) if m.mtime else None,
                                   m.mode if m.mode else None, verbose=verbose)
                continue

            # Ensure parent dir exists
            ensure_remote_dirs(sftp, str(PurePosixPath(rpath).parent), verbose=verbose)

            if kind == "symlink" and m.linkname:
                try:
                    try:
                        sftp.remove(rpath)
                    except Exception:
                        pass
                    sftp.symlink(m.linkname, rpath)
                    if verbose:
                        print(f"[LINK] {rpath} -> {m.linkname}")
                    # mtime для ссылок обычно не ставим
                    continue
                except Exception:
                    # fallback — запишем как обычный файл с содержимым ссылки (редко нужно)
                    pass

            if kind == "file":
                fsrc = tf.extractfile(m)
                if fsrc is None:
                    # пустой файл
                    fsrc_data = b""
                    with sftp.open(rpath, "wb") as dst:
                        if verbose:
                            print(f"[PUT ] {rpath} (empty)")
                        dst.write(fsrc_data)
                else:
                    with fsrc, sftp.open(rpath, "wb") as dst:
                        if verbose:
                            print(f"[PUT ] {rpath}")
                        while True:
                            chunk = fsrc.read(1024 * 1024)
                            if not chunk:
                                break
                            dst.write(chunk)
                set_times_and_mode(sftp, rpath, int(m.mtime) if m.mtime else None,
                                   m.mode if m.mode else None, verbose=verbose)
                continue

            # Остальные типы (hardlink и т.п.) — по ситуации пропустим
            if verbose:
                print(f"[SKIP] unsupported member type {m.type} for {name}")


# ----------------------------- main -----------------------------

def main():
    # Грузим конфиг рядом (если есть)
    default_cfg = {}
    here = Path(__file__).resolve().parent
    for fname in ("config.yaml", "config.yml", "config.json"):
        p = here / fname
        if p.exists():
            default_cfg = load_config(p)
            print(f"[INFO] Загружен конфиг: {p}")
            break

    ap = argparse.ArgumentParser(
        description="Заливает файлы из локального архива на Linux VM в указанную папку (по SFTP)."
    )
    ap.add_argument("--host", default=default_cfg.get("host"))
    ap.add_argument("--port", type=int, default=default_cfg.get("port", 22))
    ap.add_argument("--user", default=default_cfg.get("user"))

    auth = ap.add_mutually_exclusive_group()
    auth.add_argument("--keyfile", default=default_cfg.get("keyfile"))
    auth.add_argument("--password", default=default_cfg.get("password"))

    ap.add_argument("--remote-dir", default=default_cfg.get("remote_dir"))
    ap.add_argument("--archive", default=default_cfg.get("archive"))
    ap.add_argument("--strip-top-level", action="store_true",
                    default=bool(default_cfg.get("strip_top_level", False)),
                    help="Если в архиве единственный верхний каталог — срезать его.")
    ap.add_argument("--verbose", action="store_true", default=bool(default_cfg.get("verbose", False)))

    args = ap.parse_args()

    # Валидация
    missing = [k for k in ("host", "user", "remote_dir", "archive") if not getattr(args, k)]
    if missing:
        ap.error("Нужно указать: --host, --user, --remote-dir, --archive (или задать их в config.*)")

    archive = Path(args.archive).expanduser().resolve()
    if not archive.exists():
        print(f"[ОШИБКА] Архив не найден: {archive}", file=sys.stderr)
        sys.exit(1)

    # Подключение SSH/SFTP
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

        # Нормализуем целевую директорию (и создадим её)
        remote_root = sftp.normalize(args.remote_dir)
        ensure_remote_dirs(sftp, remote_root, verbose=args.verbose)

        # Отправка по типу архива
        lower = archive.name.lower()
        if lower.endswith(".zip"):
            upload_zip(sftp, archive, remote_root, args.strip_top_level, verbose=args.verbose)
        elif lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
            upload_tar(sftp, archive, remote_root, args.strip_top_level, verbose=args.verbose)
        else:
            print(f"[ОШИБКА] Неподдерживаемый тип архива: {archive.name}", file=sys.stderr)
            sys.exit(2)

        print(f"[ГОТОВО] Архив {archive.name} разложен в {remote_root}")

        sftp.close()
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

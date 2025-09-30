import argparse
import fnmatch
import json
import os
import stat
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional, List, Tuple, Iterable, Dict

import paramiko

try:
    import yaml  # опционально: pip install pyyaml
    HAVE_YAML = True
except Exception:
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

# ----------------------------- utils -----------------------------

def normalize_arcpath(p: str) -> str:
    p = p.replace("\\", "/")
    # убрать абсолют / и ./ спереди
    while p.startswith("/") or p.startswith("./"):
        p = p[1:] if p.startswith("/") else p[2:]
    # убрать повторяющиеся слэши
    while "//" in p:
        p = p.replace("//", "/")
    return p

def is_safe_join(root: PurePosixPath, rel: PurePosixPath) -> bool:
    try:
        return str(root.joinpath(rel)).startswith(str(root))
    except Exception:
        return False

def ensure_remote_dirs(sftp: paramiko.SFTPClient, path: str, verbose=False):
    parts = PurePosixPath(path).parts
    cur = PurePosixPath("/")
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
            pass  # уже есть

def set_times_and_mode(sftp: paramiko.SFTPClient, rpath: str,
                       mtime: Optional[int], mode: Optional[int], verbose=False):
    try:
        if mtime is not None:
            sftp.utime(rpath, (mtime, mtime))
    except Exception:
        if verbose:
            print(f"[WARN] utime failed for {rpath}")
    try:
        if mode is not None:
            sftp.chmod(rpath, mode & 0o7777)
    except Exception:
        if verbose:
            print(f"[WARN] chmod failed for {rpath}")

def first_top_level(names: Iterable[str]) -> Optional[str]:
    tops = set()
    for n in names:
        top = normalize_arcpath(n).split("/", 1)[0]
        if top:
            tops.add(top)
        if len(tops) > 1:
            return None
    return next(iter(tops)) if tops else None

# ----------------------------- routing -----------------------------

@dataclass
class FileRoute:
    src_prefix: str   # относительный префикс внутри архива
    dst_root: str     # абсолютная папка на VM

def build_routes(map_cfg: List[Dict]) -> List[FileRoute]:
    routes = []
    for item in map_cfg or []:
        src = normalize_arcpath(str(item.get("from", "")).strip().strip("/"))
        dst = str(item.get("to", "")).strip()
        if not src or not dst:
            continue
        routes.append(FileRoute(src_prefix=src, dst_root=dst))
    # длинные префиксы раньше, чтобы не перехватывали короткие
    routes.sort(key=lambda r: len(r.src_prefix), reverse=True)
    return routes

def resolve_destination(rel_path: str, routes: List[FileRoute], default_root: str) -> Tuple[str, str]:
    """
    Возвращает (remote_dir, rel_inside_remote_dir) для файла rel_path.
    Если попал под маршрут — remote_dir=route.dst_root, иначе remote_dir=default_root.
    """
    norm = normalize_arcpath(rel_path)
    for r in routes:
        if norm == r.src_prefix or norm.startswith(r.src_prefix + "/"):
            tail = norm[len(r.src_prefix):].lstrip("/")
            return r.dst_root, tail
    return default_root, norm

# ----------------------------- uploaders -----------------------------

def detect_zip_unix_mode(zi: zipfile.ZipInfo) -> Tuple[Optional[int], bool]:
    ext = zi.external_attr >> 16
    mode = ext & 0o7777 if ext else None
    is_symlink = (ext & 0o170000) == stat.S_IFLNK
    return mode, is_symlink

def upload_zip(
    sftp: paramiko.SFTPClient,
    zip_path: Path,
    base_remote_root: str,
    routes: List[FileRoute],
    strip_top_level: bool,
    verbose: bool=False
):
    with zipfile.ZipFile(zip_path, "r") as zf:
        top = first_top_level([i.filename for i in zf.infolist()])
        for zi in zf.infolist():
            name = normalize_arcpath(zi.filename)
            if not name:
                continue

            # каталоги в zip имеют хвост "/"
            is_dir = name.endswith("/")
            rel = name[:-1] if is_dir else name

            if strip_top_level and top and rel.startswith(top + "/"):
                rel = rel[len(top) + 1:]
            if not rel:
                continue

            remote_root, tail = resolve_destination(rel, routes, base_remote_root)
            rpath = str(PurePosixPath(remote_root) / tail)

            # защита
            if not is_safe_join(PurePosixPath(remote_root), PurePosixPath(tail)):
                if verbose:
                    print(f"[SKIP] unsafe path: {rel}")
                continue

            if is_dir:
                ensure_remote_dirs(sftp, rpath, verbose=verbose)
                mode, _ = detect_zip_unix_mode(zi)
                set_times_and_mode(sftp, rpath, None, mode, verbose=verbose)
                continue

            # файл / симлинк
            ensure_remote_dirs(sftp, str(PurePosixPath(rpath).parent), verbose=verbose)
            mode, is_link = detect_zip_unix_mode(zi)

            if is_link:
                # содержимое zip-элемента — целевая строка ссылки
                try:
                    link_target = zf.read(zi).decode("utf-8", "surrogateescape")
                    try:
                        sftp.remove(rpath)
                    except Exception:
                        pass
                    sftp.symlink(link_target, rpath)
                    if verbose:
                        print(f"[LINK] {rpath} -> {link_target}")
                    continue
                except Exception:
                    pass  # упадём в запись как обычного файла

            with zf.open(zi, "r") as src, sftp.open(rpath, "wb") as dst:
                if verbose:
                    print(f"[PUT ] {rpath}")
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

            # время из zip (без TZ)
            try:
                import time as _time, datetime as _dt
                dt_tuple = zi.date_time
                mtime = int(_time.mktime(_dt.datetime(*dt_tuple).timetuple()))
            except Exception:
                mtime = None
            set_times_and_mode(sftp, rpath, mtime, mode, verbose=verbose)

def upload_tar(
    sftp: paramiko.SFTPClient,
    tar_path: Path,
    base_remote_root: str,
    routes: List[FileRoute],
    strip_top_level: bool,
    verbose: bool=False
):
    with tarfile.open(tar_path, "r:*") as tf:
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

            remote_root, tail = resolve_destination(rel, routes, base_remote_root)
            rpath = str(PurePosixPath(remote_root) / tail)
            if not is_safe_join(PurePosixPath(remote_root), PurePosixPath(tail)):
                if verbose:
                    print(f"[SKIP] unsafe path: {rel}")
                continue

            t = m.type
            if t == tarfile.DIRTYPE:
                ensure_remote_dirs(sftp, rpath, verbose=verbose)
                set_times_and_mode(sftp, rpath, int(m.mtime) if m.mtime else None,
                                   m.mode if m.mode else None, verbose=verbose)
                continue

            ensure_remote_dirs(sftp, str(PurePosixPath(rpath).parent), verbose=verbose)

            if t == tarfile.SYMTYPE and m.linkname:
                try:
                    try:
                        sftp.remove(rpath)
                    except Exception:
                        pass
                    sftp.symlink(m.linkname, rpath)
                    if verbose:
                        print(f"[LINK] {rpath} -> {m.linkname}")
                    continue
                except Exception:
                    pass

            if t in (tarfile.AREGTYPE, tarfile.REGTYPE):
                fsrc = tf.extractfile(m)
                if fsrc is None:
                    with sftp.open(rpath, "wb") as dst:
                        if verbose:
                            print(f"[PUT ] {rpath} (empty)")
                        dst.write(b"")
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

            if verbose:
                print(f"[SKIP] unsupported tar member type {t} for {name}")

# ----------------------------- DB restore -----------------------------

def shquote(x: str) -> str:
    return x

def run_sql_via_psql(
    ssh: paramiko.SSHClient,
    sql_stream,
    db_cfg: dict,
    verbose: bool=False
):
    """
    Качает SQL в stdin psql на VM.
    db_cfg: {psql_path, db_host, db_port, db_name, db_user, db_password, create_db_if_missing, drop_before}
    """
    psql_path = db_cfg.get("psql_path", "psql")
    db_host = str(db_cfg.get("db_host", "127.0.0.1"))
    db_port = int(db_cfg.get("db_port", 5432))
    db_name = str(db_cfg.get("db_name"))
    db_user = str(db_cfg.get("db_user"))
    db_password = db_cfg.get("db_password")
    create_db = bool(db_cfg.get("create_db_if_missing", False))
    drop_before = bool(db_cfg.get("drop_before", False))

    if not db_name or not db_user:
        raise ValueError("Для восстановления БД нужны db_name и db_user")
    
    env_prefix = f"PGPASSWORD={shquote(db_password)} " if db_password else ""
    full_cmd = shquote(env_prefix + f"dropdb -h {shquote(db_host)} -p {db_port} -U {shquote(db_user)} {shquote(db_name)}")
    _, stdout_c, stderr_c = ssh.exec_command(full_cmd)
    code = stdout_c.channel.recv_exit_status()
    if code != 0:
        err = stderr_c.read().decode("utf-8", "ignore")
        raise RuntimeError(f"Удаление БД завершилось с кодом {code}:\n{err}")
    
    full_cmd = shquote(env_prefix + f"createdb -h {shquote(db_host)} -p {db_port} -U {shquote(db_user)} {shquote(db_name)}")
    _, stdout_c, stderr_c = ssh.exec_command(full_cmd)
    code = stdout_c.channel.recv_exit_status()
    if code != 0:
        err = stderr_c.read().decode("utf-8", "ignore")
        raise RuntimeError(f"создание БД завершилось с кодом {code}:\n{err}")

    # Основной импорт
    base_cmd = (
        f"{psql_path} -h {shquote(db_host)} -p {db_port} -U {shquote(db_user)} "
        f"-d {shquote(db_name)} -v ON_ERROR_STOP=1"
    )
    
    full_cmd = shquote(env_prefix + base_cmd)
    if verbose:
        print(f"[INFO] psql restore cmd: {full_cmd}")

    stdin, stdout, stderr = ssh.exec_command(full_cmd)
    try:
        while True:
            chunk = sql_stream.read(1024 * 1024)
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8", "ignore")
            stdin.write(chunk)
        stdin.channel.shutdown_write()
    finally:
        # дождёмся завершения
        code = stdout.channel.recv_exit_status()
        if code != 0:
            err = stderr.read().decode("utf-8", "ignore")
            raise RuntimeError(f"psql вернул код {code}:\n{err}")

# ----------------------------- main -----------------------------

def main():
    # Конфиг рядом
    default_cfg = {}
    here = Path(__file__).resolve().parent
    for fname in ("config.yaml", "config.yml", "config.json"):
        p = here / fname
        if p.exists():
            default_cfg = load_config(p)
            print(f"[INFO] Загружен конфиг: {p}")
            break

    ap = argparse.ArgumentParser(
        description="Разворачивает файлы из архива на Linux VM (по \"нужным местам\") и восстанавливает БД PostgreSQL из .sql внутри архива."
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
                    default=bool(default_cfg.get("strip_top_level", False)))
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

    # Маршрутизация
    routes = build_routes(default_cfg.get("file_map", []))

    # Подключение
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

        # Нормализуем базовую директорию (создадим её)
        base_remote_root = sftp.normalize(args.remote_dir)
        ensure_remote_dirs(sftp, base_remote_root, verbose=args.verbose)

        lower = archive.name.lower()
        if lower.endswith(".zip"):
            upload_zip(sftp, archive, base_remote_root, routes, args.strip_top_level, verbose=args.verbose)
        elif lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
            upload_tar(sftp, archive, base_remote_root, routes, args.strip_top_level, verbose=args.verbose)
        else:
            print(f"[ОШИБКА] Неподдерживаемый тип архива: {archive.name}", file=sys.stderr)
            sys.exit(2)

        # --- DB restore, если включено ---
        db_cfg = default_cfg.get("db_restore", {}) or {}
        if db_cfg.get("enabled"):
            # найдём SQL в архиве по glob
            sql_glob = db_cfg.get("sql_glob", "*.sql")
            sql_member_name = None

            if lower.endswith(".zip"):
                with zipfile.ZipFile(archive, "r") as zf:
                    names = [n for n in (i.filename for i in zf.infolist())
                             if fnmatch.fnmatch(normalize_arcpath(n), sql_glob)]
                    if names:
                        names.sort()  # последний по имени
                        sql_member_name = names[-1]
                        if args.strip_top_level:
                            top = first_top_level(names)
                            # не обязательно срезать для SQL, он и так относительный
                    if not sql_member_name:
                        raise FileNotFoundError(f"В архиве нет SQL по шаблону {sql_glob}")
                    if args.verbose:
                        print(f"[INFO] Восстанавливаю БД из: {sql_member_name}")
                    with zf.open(sql_member_name, "r") as sql_stream:
                        run_sql_via_psql(client, sql_stream, db_cfg, verbose=args.verbose)

            else:  # tar
                with tarfile.open(archive, "r:*") as tf:
                    names = [m.name for m in tf.getmembers()
                             if fnmatch.fnmatch(normalize_arcpath(m.name), sql_glob)]
                    if names:
                        names.sort()
                        sql_member_name = names[-1]
                    if not sql_member_name:
                        raise FileNotFoundError(f"В архиве нет SQL по шаблону {sql_glob}")
                    if args.verbose:
                        print(f"[INFO] Восстанавливаю БД из: {sql_member_name}")
                    m = tf.getmember(sql_member_name)
                    fsrc = tf.extractfile(m)
                    if fsrc is None:
                        raise RuntimeError(f"Не удалось открыть {sql_member_name} в tar")
                    with fsrc:
                        run_sql_via_psql(client, fsrc, db_cfg, verbose=args.verbose)

        print("[ГОТОВО] Файлы разложены. Восстановление БД (если включено) — выполнено.")
        sftp.close()
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

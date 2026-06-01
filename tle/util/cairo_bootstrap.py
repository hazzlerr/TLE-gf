import os
import re
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path


_MIN_VERSION = (1, 18, 0)
_DEFAULT_VERSION = '1.18.4'
_SOURCE_URL = 'https://www.cairographics.org/releases/cairo-{version}.tar.xz'
_VERSION_RE = re.compile(r'^\d+\.\d+\.\d+$')
_FAILURE_BACKOFF_SECONDS = 24 * 60 * 60
_TRUE_VALUES = {'1', 'true', 'yes', 'on'}


def _repo_root():
    return Path(__file__).resolve().parents[2]


def _parse_version(version):
    parts = [int(part) for part in re.findall(r'\d+', version or '')[:3]]
    return tuple((parts + [0, 0, 0])[:3])


def _version_at_least(version, minimum=_MIN_VERSION):
    return _parse_version(version) >= minimum


def _validate_version(version):
    if not _VERSION_RE.fullmatch(version):
        raise ValueError(f'invalid Cairo version: {version}')
    return version


def _prepend_env(env, name, paths):
    existing = env.get(name)
    values = [str(path) for path in paths if path]
    if existing:
        values.append(existing)
    env[name] = os.pathsep.join(values)


def _prefix_env(prefix):
    env = os.environ.copy()
    _prepend_env(env, 'LD_LIBRARY_PATH', [prefix / 'lib'])
    _prepend_env(
        env,
        'PKG_CONFIG_PATH',
        [prefix / 'lib' / 'pkgconfig', prefix / 'share' / 'pkgconfig'],
    )
    return env


def _python_cairo_version(env):
    try:
        return subprocess.check_output(
            [
                sys.executable,
                '-c',
                'import cairo; print(cairo.cairo_version_string())',
            ],
            env=env,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def _emit_exports(env):
    for key in ('LD_LIBRARY_PATH', 'PKG_CONFIG_PATH'):
        if env.get(key):
            print(f'{key}={env[key]}')
    print('TLE_ALLOW_COLOR_EMOJI=1')


def _run(cmd, **kwargs):
    subprocess.run(cmd, check=True, stdout=sys.stderr, stderr=sys.stderr, **kwargs)


def _download_source(archive, version):
    if archive.exists():
        return
    archive.parent.mkdir(parents=True, exist_ok=True)
    tmp_archive = archive.with_suffix(archive.suffix + '.part')
    try:
        with urllib.request.urlopen(_SOURCE_URL.format(version=version), timeout=60) as resp:
            tmp_archive.write_bytes(resp.read())
        tmp_archive.replace(archive)
    finally:
        if tmp_archive.exists():
            tmp_archive.unlink()


def _extract_source(archive, source_dir):
    if source_dir.exists():
        return
    with tarfile.open(archive) as tar:
        _validate_tar_members(tar, source_dir.parent)
        tar.extractall(source_dir.parent)


def _validate_tar_members(tar, destination):
    destination = destination.resolve()
    for member in tar.getmembers():
        member_path = (destination / member.name).resolve()
        try:
            member_path.relative_to(destination)
        except ValueError as e:
            raise ValueError(f'archive member escapes destination: {member.name}') from e
        if member.issym() or member.islnk():
            raise ValueError(f'archive links are not supported: {member.name}')


def _cache_dir():
    return _repo_root() / 'data' / 'assets' / 'cairo'


def _failure_stamp(version):
    return _cache_dir() / f'cairo-{version}.failed'


def _failure_backoff_active(stamp):
    if not stamp.exists():
        return False
    return time.time() - stamp.stat().st_mtime < _FAILURE_BACKOFF_SECONDS


def _record_failure(stamp, error):
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text(f'{time.ctime()}: {error}\n')


def _clear_failure(stamp):
    try:
        stamp.unlink()
    except FileNotFoundError:
        pass


def _build_cairo(prefix, version):
    root = _repo_root()
    cache_dir = _cache_dir()
    build_root = root / 'data' / 'temp' / 'cairo-build'
    archive = cache_dir / f'cairo-{version}.tar.xz'
    source_dir = cache_dir / f'cairo-{version}'
    build_dir = build_root / f'cairo-{version}'

    _download_source(archive, version)
    _extract_source(archive, source_dir)

    _run([sys.executable, '-m', 'pip', 'install', '--quiet', 'meson', 'ninja'])

    setup_cmd = [
        sys.executable,
        '-m',
        'mesonbuild.mesonmain',
        'setup',
        '--prefix',
        str(prefix),
        '--libdir',
        'lib',
        '--buildtype',
        'release',
        str(build_dir),
        str(source_dir),
    ]
    if build_dir.exists():
        setup_cmd.insert(-2, '--wipe')
    _run(setup_cmd)
    _run([sys.executable, '-m', 'mesonbuild.mesonmain', 'compile', '-C', str(build_dir)])
    _run([sys.executable, '-m', 'mesonbuild.mesonmain', 'install', '-C', str(build_dir)])


def _try_prefix(prefix):
    env = _prefix_env(prefix)
    version = _python_cairo_version(env)
    if version and _version_at_least(version):
        _emit_exports(env)
        return True
    return False


def main():
    if os.environ.get('TLE_CAIRO_BOOTSTRAP', '1') == '0':
        return 0

    current_version = _python_cairo_version(os.environ.copy())
    if current_version and _version_at_least(current_version):
        print('TLE_ALLOW_COLOR_EMOJI=1')
        return 0

    try:
        version = _validate_version(os.environ.get('TLE_CAIRO_BOOTSTRAP_VERSION', _DEFAULT_VERSION))
    except ValueError as e:
        print(f'Local Cairo bootstrap disabled: {e}', file=sys.stderr)
        return 0

    prefix = Path(os.environ.get(
        'TLE_CAIRO_PREFIX',
        _repo_root() / 'data' / 'assets' / 'cairo' / f'cairo-{version}',
    ))

    if _try_prefix(prefix):
        return 0

    stamp = _failure_stamp(version)
    force = os.environ.get('TLE_CAIRO_BOOTSTRAP_FORCE', '').lower() in _TRUE_VALUES
    if not force and _failure_backoff_active(stamp):
        print(
            f'Local Cairo bootstrap recently failed for {version}; '
            'continuing without color emoji. Set TLE_CAIRO_BOOTSTRAP_FORCE=1 to retry now.',
            file=sys.stderr,
        )
        return 0

    print(
        f'Cairo {current_version or "unknown"} is too old for color emoji; '
        f'trying local Cairo {version} bootstrap.',
        file=sys.stderr,
    )
    try:
        _build_cairo(prefix, version)
    except Exception as e:
        _record_failure(stamp, e)
        print(f'Local Cairo bootstrap failed: {e}', file=sys.stderr)
        return 0

    if not _try_prefix(prefix):
        reason = 'local Cairo did not load in Python'
        _record_failure(stamp, reason)
        print(f'Local Cairo bootstrap finished but {reason}; continuing without color emoji.',
              file=sys.stderr)
    else:
        _clear_failure(stamp)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

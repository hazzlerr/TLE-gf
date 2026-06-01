import os
import sys
import tarfile
from io import BytesIO

from tle.util import cairo_bootstrap
from tle.util import font_config


def _clear_font_env(monkeypatch):
    for name in ('FONTCONFIG_FILE', 'TLE_ALLOW_COLOR_EMOJI', 'TLE_COLOR_EMOJI_ENABLED'):
        monkeypatch.delenv(name, raising=False)


def test_font_config_uses_monochrome_by_default(monkeypatch, tmp_path):
    _clear_font_env(monkeypatch)
    monkeypatch.setattr(font_config, '_repo_root', lambda: tmp_path)

    assert font_config.configure() is False

    assert os.environ['FONTCONFIG_FILE'] == str(tmp_path / 'extra' / 'fonts.conf')
    assert os.environ['TLE_COLOR_EMOJI_ENABLED'] == '0'


def test_font_config_rejects_color_emoji_on_old_cairo(monkeypatch, tmp_path):
    _clear_font_env(monkeypatch)
    monkeypatch.setenv('TLE_ALLOW_COLOR_EMOJI', '1')
    monkeypatch.setattr(font_config, '_repo_root', lambda: tmp_path)
    monkeypatch.setattr(
        sys.modules['cairo'], 'cairo_version_string', lambda: '1.16.0', raising=False)

    assert font_config.configure() is False

    assert os.environ['FONTCONFIG_FILE'] == str(tmp_path / 'extra' / 'fonts.conf')
    assert os.environ['TLE_COLOR_EMOJI_ENABLED'] == '0'
    assert 'older than 1.18.0' in font_config.status()['reason']


def test_font_config_allows_color_emoji_on_new_cairo(monkeypatch, tmp_path):
    _clear_font_env(monkeypatch)
    monkeypatch.setenv('TLE_ALLOW_COLOR_EMOJI', '1')
    monkeypatch.setattr(font_config, '_repo_root', lambda: tmp_path)
    monkeypatch.setattr(
        sys.modules['cairo'], 'cairo_version_string', lambda: '1.18.4', raising=False)

    assert font_config.configure() is True

    assert os.environ['FONTCONFIG_FILE'] == str(tmp_path / 'extra' / 'fonts-color.conf')
    assert os.environ['TLE_COLOR_EMOJI_ENABLED'] == '1'


def test_cairo_bootstrap_prefix_env_prepends_local_paths(monkeypatch, tmp_path):
    monkeypatch.setenv('LD_LIBRARY_PATH', '/system/lib')
    monkeypatch.setenv('PKG_CONFIG_PATH', '/system/pkgconfig')

    env = cairo_bootstrap._prefix_env(tmp_path)

    assert env['LD_LIBRARY_PATH'].split(os.pathsep)[:2] == [
        str(tmp_path / 'lib'),
        '/system/lib',
    ]
    assert env['PKG_CONFIG_PATH'].split(os.pathsep)[:3] == [
        str(tmp_path / 'lib' / 'pkgconfig'),
        str(tmp_path / 'share' / 'pkgconfig'),
        '/system/pkgconfig',
    ]


def test_cairo_bootstrap_new_system_cairo_emits_safe_env(capsys, monkeypatch):
    monkeypatch.setattr(cairo_bootstrap, '_python_cairo_version', lambda env: '1.18.4')

    assert cairo_bootstrap.main() == 0

    assert capsys.readouterr().out == 'TLE_ALLOW_COLOR_EMOJI=1\n'


def test_cairo_bootstrap_build_failure_fails_open_and_records_backoff(
        capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(cairo_bootstrap, '_repo_root', lambda: tmp_path)
    monkeypatch.setattr(cairo_bootstrap, '_python_cairo_version', lambda env: '1.16.0')
    monkeypatch.setattr(cairo_bootstrap, '_try_prefix', lambda prefix: False)
    monkeypatch.setattr(
        cairo_bootstrap, '_build_cairo',
        lambda prefix, version: (_ for _ in ()).throw(RuntimeError('missing deps')))

    assert cairo_bootstrap.main() == 0

    captured = capsys.readouterr()
    assert captured.out == ''
    assert 'Local Cairo bootstrap failed: missing deps' in captured.err
    assert (tmp_path / 'data' / 'assets' / 'cairo' / 'cairo-1.18.4.failed').exists()


def test_cairo_bootstrap_recent_failure_skips_build(capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(cairo_bootstrap, '_repo_root', lambda: tmp_path)
    monkeypatch.setattr(cairo_bootstrap, '_python_cairo_version', lambda env: '1.16.0')
    monkeypatch.setattr(cairo_bootstrap, '_try_prefix', lambda prefix: False)
    stamp = tmp_path / 'data' / 'assets' / 'cairo' / 'cairo-1.18.4.failed'
    stamp.parent.mkdir(parents=True)
    stamp.write_text('failed')

    def build(_prefix, _version):
        raise AssertionError('build should be skipped during backoff')

    monkeypatch.setattr(cairo_bootstrap, '_build_cairo', build)

    assert cairo_bootstrap.main() == 0

    captured = capsys.readouterr()
    assert captured.out == ''
    assert 'recently failed' in captured.err


def test_cairo_bootstrap_rejects_invalid_version(capsys, monkeypatch):
    monkeypatch.setenv('TLE_CAIRO_BOOTSTRAP_VERSION', '../1.18.4')
    monkeypatch.setattr(cairo_bootstrap, '_python_cairo_version', lambda env: '1.16.0')

    assert cairo_bootstrap.main() == 0

    captured = capsys.readouterr()
    assert captured.out == ''
    assert 'invalid Cairo version' in captured.err


def test_cairo_bootstrap_rejects_unsafe_tar_members(tmp_path):
    archive = tmp_path / 'bad.tar'
    with tarfile.open(archive, 'w') as tar:
        info = tarfile.TarInfo('../escape')
        data = b'x'
        info.size = len(data)
        tar.addfile(info, BytesIO(data))

    with tarfile.open(archive) as tar:
        try:
            cairo_bootstrap._validate_tar_members(tar, tmp_path / 'dest')
        except ValueError as e:
            assert 'escapes destination' in str(e)
        else:
            raise AssertionError('unsafe archive member was accepted')

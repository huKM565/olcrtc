import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "deploy-olcrtc-vds.sh"

FAKE_SSH = r'''#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path
with open(os.environ["FAKE_REMOTE_LOG"], "a") as log:
    log.write(" ".join(sys.argv[1:]) + "\n")
if os.environ.get("FAKE_SSH_EXIT"):
    sys.stderr.write("remote rejected " + os.environ.get("FAKE_SECRET", "") + "\n")
    sys.exit(int(os.environ["FAKE_SSH_EXIT"]))
if os.environ.get("FAKE_REMOTE_ROOT") and "== OS ==" not in sys.argv[-1]:
    target = next(argument for argument in sys.argv if argument.startswith("root@"))
    root_name = "FAKE_REMOTE_ROOT_2" if target == "root@192.0.2.20" and os.environ.get("FAKE_REMOTE_ROOT_2") else "FAKE_REMOTE_ROOT"
    root = Path(os.environ[root_name])
    stdin_mode = "-s" in sys.argv
    target_index = next(index for index, argument in enumerate(sys.argv) if argument.startswith("root@"))
    command = sys.stdin.read() if stdin_mode else " ".join(sys.argv[target_index + 1:])
    for prefix in ("/etc/ilyukhaline", "/etc/olcrtc", "/etc/systemd/system", "/etc/nginx", "/usr/local/etc/xray", "/opt/ilyukhaline", "/var/lib/ilyukhaline", "/var/www", "/tmp/ilyukhaline"):
        command = command.replace(prefix, str(root) + prefix)
    env = os.environ.copy()
    env["FAKE_REMOTE_ROOT"] = str(root)
    env["PATH"] = str(root / "remote-bin") + os.pathsep + env["PATH"]
    if stdin_mode:
        arguments = sys.argv[sys.argv.index("-s") + 1:]
        arguments = [str(root) + argument if argument.startswith("/") else argument for argument in arguments]
        result = subprocess.run(["bash", "-s", *arguments], input=command, text=True, env=env)
    else:
        result = subprocess.run(["bash", "-c", command], env=env)
    sys.exit(result.returncode)
print("PRETTY_NAME=Debian GNU/Linux 13")
print("MemTotal: 2048000 kB")
print("/dev/vda1 20G 8G 12G 45% /")
print("127.0.0.1:10000")
print("ilyukhaline-olcrtc@m1.service")
print("xray.service")
print("hysteria-server.service")
'''

FAKE_SCP = r'''#!/usr/bin/env python3
import os
import shutil
import sys
from pathlib import Path
source = Path(sys.argv[-2])
host, target = sys.argv[-1].split(":", 1)
root_name = "FAKE_REMOTE_ROOT_2" if host == "root@192.0.2.20" and os.environ.get("FAKE_REMOTE_ROOT_2") else "FAKE_REMOTE_ROOT"
for prefix in ("/etc/ilyukhaline", "/etc/olcrtc", "/etc/systemd/system", "/etc/nginx", "/usr/local/etc/xray", "/opt/ilyukhaline", "/var/lib/ilyukhaline", "/var/www", "/tmp/ilyukhaline"):
    if target.startswith(prefix):
        target = os.environ[root_name] + target
        break
path = Path(target)
path.parent.mkdir(parents=True, exist_ok=True)
shutil.copyfile(source, path)
shutil.copystat(source, path)
'''

FAKE_SYSTEMCTL = r'''#!/usr/bin/env python3
import os
import sys
from pathlib import Path
root = Path(os.environ["FAKE_REMOTE_ROOT"])
log = root / "systemctl.log"
with log.open("a") as stream:
    stream.write(" ".join(sys.argv[1:]) + "\n")
action = sys.argv[1] if len(sys.argv) > 1 else ""
units = [value for value in sys.argv[2:] if value.endswith(".service")]
if action == "show" and units:
    status = root / f"{units[0]}.status"
    if status.exists():
        print(status.read_text(), end="")
    sys.exit(0)
active = root / "active-units"
current = set(active.read_text().splitlines()) if active.exists() else set()
if action in ("start", "restart", "enable"):
    current.update(unit for unit in units if unit != os.environ.get("FAKE_INACTIVE_UNIT"))
elif action in ("stop", "disable"):
    current.difference_update(units)
elif action == "is-active":
    state = "active" if units and units[0] in current else "inactive"
    print(state)
    sys.exit(0 if state == "active" else 3)
active.write_text("\n".join(sorted(current)) + ("\n" if current else ""))
'''

FAKE_SYSTEMD_RUN = r'''#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path
root = Path(os.environ["FAKE_REMOTE_ROOT"])
unit = next(value.split("=", 1)[1] for value in sys.argv if value.startswith("--unit="))
command = sys.argv[sys.argv.index("--no-block") + 1:]
if command[0] == "bash":
    script = Path(command[1]).read_text()
    for prefix in ("/etc/ilyukhaline", "/etc/olcrtc", "/etc/systemd/system", "/etc/nginx", "/opt/ilyukhaline", "/usr/local/etc/xray", "/var/lib/ilyukhaline", "/var/www", "/tmp/ilyukhaline"):
        script = script.replace(prefix, str(root) + prefix)
    command = ["bash", "-s", *command[2:]]
log = root / f"{unit}.log"
with log.open("w") as stream:
    result = subprocess.run(command, input=script, text=True, stdout=stream, stderr=subprocess.STDOUT)
status = "failed" if result.returncode else "active"
(root / f"{unit}.status").write_text(f"ActiveState={status}\nExecMainStatus={result.returncode}\n")
'''

FAKE_JOURNALCTL = r'''#!/usr/bin/env python3
import os
import sys
from pathlib import Path
root = Path(os.environ["FAKE_REMOTE_ROOT"])
unit = sys.argv[sys.argv.index("-u") + 1]
log = root / f"{unit}.log"
if log.exists():
    print(log.read_text(), end="")
'''

FAKE_MAGE = r'''#!/usr/bin/env python3
import os
import sys
from pathlib import Path
with open(os.environ["FAKE_BUILD_LOG"], "a") as log:
    log.write(" ".join(sys.argv[1:]) + " GOOS=" + os.environ.get("GOOS", "") + " GOARCH=" + os.environ.get("GOARCH", "") + " CGO=" + os.environ.get("CGO_ENABLED", "") + "\n")
if sys.argv[1:] == ["build"]:
    output = Path("build/olcrtc-linux-amd64")
    output.parent.mkdir(exist_ok=True)
    output.write_text("#!/usr/bin/env sh\necho fake-olcrtc\n")
    output.chmod(0o755)
'''

FAKE_XRAY = r'''#!/usr/bin/env python3
import json
import os
import sys
if "-test" in sys.argv:
    config = sys.argv[sys.argv.index("-config") + 1]
    with open(config) as stream:
        json.load(stream)
    if os.environ.get("FAKE_XRAY_TEST_FAIL"):
        print("intentional invalid xray config", file=sys.stderr)
        sys.exit(1)
'''


class DeployPreflightTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        ssh = self.bin / "ssh"
        ssh.write_text(FAKE_SSH)
        ssh.chmod(0o755)
        scp = self.bin / "scp"
        scp.write_text(FAKE_SCP)
        scp.chmod(0o755)
        self.remote_log = self.root / "remote.log"
        self.config = self.root / "vds.conf"
        self.write_config({})
        self.env = os.environ.copy()
        self.env.update({
            "PATH": f"{self.bin}{os.pathsep}{self.env['PATH']}",
            "FAKE_REMOTE_LOG": str(self.remote_log),
        })

    def tearDown(self):
        self.temp.cleanup()

    def install_fake_nginx(self, root):
        nginx = root / "remote-bin/nginx"
        nginx.write_text("#!/usr/bin/env sh\nexit \"${FAKE_NGINX_EXIT:-0}\"\n")
        nginx.chmod(0o755)
        certbot = root / "remote-bin/certbot"
        certbot.write_text("#!/usr/bin/env sh\nexit 0\n")
        certbot.chmod(0o755)
        chgrp = root / "remote-bin/chgrp"
        chgrp.write_text("#!/usr/bin/env sh\nprintf '%s\\n' \"$*\" >> \"${FAKE_REMOTE_ROOT}/chgrp.log\"\n")
        chgrp.chmod(0o755)
        systemd_run = root / "remote-bin/systemd-run"
        systemd_run.write_text(FAKE_SYSTEMD_RUN)
        systemd_run.chmod(0o755)
        journalctl = root / "remote-bin/journalctl"
        journalctl.write_text(FAKE_JOURNALCTL)
        journalctl.chmod(0o755)

    def enable_remote(self):
        self.remote = self.root / "remote"
        remote_bin = self.remote / "remote-bin"
        remote_bin.mkdir(parents=True)
        systemctl = remote_bin / "systemctl"
        systemctl.write_text(FAKE_SYSTEMCTL)
        systemctl.chmod(0o755)
        self.install_fake_nginx(self.remote)
        mage = self.bin / "mage"
        mage.write_text(FAKE_MAGE)
        mage.chmod(0o755)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "magefile.go").write_text("package main\n")
        self.xray = self.root / "xray"
        self.xray.write_text(FAKE_XRAY)
        self.xray.chmod(0o755)
        self.build_log = self.root / "build.log"
        self.write_config({
            "OLCRTC_SOURCE": str(self.source),
            "XRAY_BINARY": str(self.xray),
        })
        self.env.update({
            "FAKE_REMOTE_ROOT": str(self.remote),
            "FAKE_BUILD_LOG": str(self.build_log),
        })

    def values(self):
        result = {
            "SSH_TARGET": "root@192.0.2.10",
            "ARTIFACT_DIR": str(self.root / "artifacts"),
            "PUBLISH_PATH": "/var/www/ilyukhaline/private.txt",
            "PUBLISH_URL": "https://example.invalid/private.txt",
            "VLESS_UUID": "11111111-1111-1111-1111-111111111111",
            "VLESS_LISTEN": "127.0.0.1:10000",
            "OLCRTC_SOURCE": str(self.root / "source"),
            "XRAY_BINARY": str(self.root / "xray"),
            "NGINX_SERVER_FILE": "/etc/nginx/sites-enabled/default",
        }
        key_patterns = {"MAMBA": "01", "VKCALLS": "02", "TELEMOST": "03"}
        for index in range(1, 5):
            result[f"MAMBA_URL_{index}"] = f"https://mamba.example.invalid/room-{index}"
            result[f"VKCALLS_URL_{index}"] = f"https://vk.com/call/join/fake-room-{index}"
            result[f"TELEMOST_URL_{index}"] = f"https://telemost.yandex.ru/j/permanent-room-{index}"
            for provider in ("MAMBA", "VKCALLS", "TELEMOST"):
                result[f"{provider}_KEY_{index}"] = key_patterns[provider] * 32
        return result

    def config_text(self, changes):
        values = self.values()
        values.update(changes)
        return "".join(f"{key}='{value}'\n" for key, value in values.items())

    def write_config(self, changes, remove=()):
        values = self.values()
        values.update(changes)
        for key in remove:
            values.pop(key, None)
        self.config.write_text("".join(f"{key}='{value}'\n" for key, value in values.items()))

    def salutejazz_rooms_text(self, duplicate=False, partial=False, mismatched_key=False):
        key = "04" * 32
        lines = []
        for index in range(1, 3 if partial else 5):
            room = "room1:pass1word" if duplicate and index == 2 else f"room{index}:pass{index}word"
            room_key = "05" * 32 if mismatched_key and index == 2 else key
            lines.append(f"SALUTEJAZZ_URL_{index}='{room}'")
            lines.append(f"SALUTEJAZZ_KEY_{index}='{room_key}'")
        return "\n".join(lines) + "\n"

    def enable_salutejazz(self, content=None):
        rooms = self.root / "salutejazz-rooms.conf"
        rooms.write_text(content if content is not None else self.salutejazz_rooms_text())
        rooms.chmod(0o600)
        self.write_config({
            "TELEMOST_ENABLED": "false",
            "SALUTEJAZZ_ENABLED": "true",
            "SALUTEJAZZ_ROOMS_FILE": str(rooms),
        })
        return rooms

    def run_script(self, *args, env=None):
        actual_env = self.env.copy()
        if env:
            actual_env.update(env)
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            text=True,
            capture_output=True,
            env=actual_env,
            timeout=10,
        )

    def assert_invalid(self, changes=None, remove=(), expected=""):
        self.write_config(changes or {}, remove)
        result = self.run_script("--config", str(self.config), "check")
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(expected, result.stderr)

    def test_explicit_config_and_supported_command_are_required(self):
        self.assertNotEqual(0, self.run_script("check").returncode)
        self.assertNotEqual(0, self.run_script("--config", str(self.config), "destroy").returncode)

    def test_missing_config_field_is_named_without_secrets(self):
        secret = self.values()["MAMBA_KEY_3"]
        self.assert_invalid(remove=("MAMBA_KEY_3",), expected="MAMBA_KEY_3")
        result = self.run_script("--config", str(self.config), "check")
        self.assertNotIn(secret, result.stderr)

    def test_each_provider_requires_four_room_urls(self):
        self.assert_invalid(remove=("VKCALLS_URL_4",), expected="VKCALLS_URL_4")

    def test_telemost_rejects_temporary_or_noncanonical_urls(self):
        self.assert_invalid(
            {"TELEMOST_URL_2": "https://telemost.yandex.ru/j/temporary?expires=86400"},
            expected="TELEMOST_URL_2",
        )

    def test_duplicate_provider_and_room_are_rejected(self):
        self.assert_invalid(
            {"MAMBA_URL_2": self.values()["MAMBA_URL_1"]},
            expected="duplicate",
        )

    def test_homogeneous_rooms_require_one_shared_key(self):
        self.assert_invalid({"MAMBA_KEY_2": "ff" * 32}, expected="keys must match")

    def test_vless_must_listen_on_loopback(self):
        self.assert_invalid({"VLESS_LISTEN": "0.0.0.0:10000"}, expected="loopback")
        self.assert_invalid({"VLESS_LISTEN": "192.0.2.10:10000"}, expected="loopback")

    def test_public_compatibility_is_explicit_and_subscription_stays_loopback(self):
        self.write_config({"VLESS_LISTEN": "0.0.0.0:10000", "VLESS_PUBLIC_COMPAT": "true"})
        result = self.run_script("--config", str(self.config), "check")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        spec = importlib.util.spec_from_file_location("subscription_generator", HERE.parent / "deploy/generate_subscription.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        link = module.vless_link({"host": "0.0.0.0", "port": 10000, "id": self.values()["VLESS_UUID"]})
        self.assertIn("@127.0.0.1:10000", link)
        self.assertNotIn("0.0.0.0:10000", link)

    def test_check_only_issues_read_only_remote_commands(self):
        result = self.run_script("--config", str(self.config), "check")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        commands = self.remote_log.read_text()
        self.assertIn("PRETTY_NAME", commands)
        self.assertIn("MemTotal", commands)
        self.assertIn("ss -ltn", commands)
        self.assertIn("systemctl list-units", commands)
        for forbidden in ("mkdir", "systemctl start", "systemctl restart", "systemctl stop", "scp ", "tee ", "rm "):
            self.assertNotIn(forbidden, commands)
        self.assertIn("Debian", result.stdout)
        self.assertIn("hysteria-server", result.stdout)

    def test_ssh_failure_is_reported_with_secrets_redacted(self):
        secret = self.values()["MAMBA_KEY_1"]
        result = self.run_script(
            "--config", str(self.config), "check",
            env={"FAKE_SSH_EXIT": "42", "FAKE_SECRET": secret},
        )
        self.assertEqual(42, result.returncode)
        self.assertNotIn(secret, result.stdout + result.stderr)
        self.assertIn("[REDACTED]", result.stderr)


class DeployLifecycleTest(DeployPreflightTest):
    def setUp(self):
        super().setUp()
        self.enable_remote()

    def build(self):
        result = self.run_script("--config", str(self.config), "build")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def deploy(self, env=None):
        return self.run_script("--config", str(self.config), "deploy", env=env)

    def managed_snapshot(self):
        result = {}
        for relative in ("etc/ilyukhaline", "opt/ilyukhaline", "etc/systemd/system"):
            base = self.remote / relative
            if base.exists():
                for path in base.rglob("*"):
                    if path.is_file():
                        result[str(path.relative_to(self.remote))] = path.read_bytes()
        return result

    def test_build_produces_static_linux_amd64_artifacts_and_hashes(self):
        self.build()
        artifacts = self.root / "artifacts"
        self.assertTrue((artifacts / "olcrtc-linux-amd64").is_file())
        self.assertTrue((artifacts / "xray").is_file())
        self.assertEqual(64, len((artifacts / "olcrtc-linux-amd64.sha256").read_text().split()[0]))
        build = self.build_log.read_text()
        self.assertIn("GOOS=linux", build)
        self.assertIn("GOARCH=amd64", build)
        self.assertIn("CGO=0", build)

    def test_deploy_is_complete_and_idempotent_without_touching_unmanaged_vpn(self):
        unmanaged = self.remote / "etc/systemd/system/hysteria-server.service"
        unmanaged.parent.mkdir(parents=True)
        unmanaged.write_text("[Service]\nExecStart=/usr/bin/hysteria\n")
        self.build()
        first = self.deploy()
        self.assertEqual(0, first.returncode, first.stdout + first.stderr)
        etc = self.remote / "etc/ilyukhaline"
        expected = [f"{provider}{index}.yaml" for provider in ("m", "v", "t") for index in range(1, 5)]
        self.assertEqual(sorted(expected + ["xray.json"]), sorted(path.name for path in etc.iterdir()))
        for provider in ("mamba", "vkcalls", "telemost"):
            rooms = [self.values()[f"{provider.upper()}_URL_{index}"] for index in range(1, 5)]
            self.assertEqual(4, len(set(rooms)))
            for room in rooms:
                self.assertTrue(any(room.encode() in path.read_bytes() for path in etc.glob("*.yaml")))
        self.assertIn('"port": 10000', (etc / "xray.json").read_text())
        for path in etc.glob("*.yaml"):
            self.assertIn("egress:\n  allow: '127.0.0.1:10000'\n", path.read_text())
        unit = self.remote / "etc/systemd/system/ilyukhaline-olcrtc@.service"
        self.assertIn("RestartSec=30s", unit.read_text())
        vk_unit = self.remote / "etc/systemd/system/ilyukhaline-olcrtc@v1.service.d/restart.conf"
        self.assertIn("RestartSec=300s", vk_unit.read_text())
        snapshot = self.managed_snapshot()
        active_before = (self.remote / "active-units").read_bytes()
        second = self.deploy()
        self.assertEqual(0, second.returncode, second.stdout + second.stderr)
        self.assertEqual(snapshot, self.managed_snapshot())
        self.assertEqual(active_before, (self.remote / "active-units").read_bytes())
        self.assertEqual("[Service]\nExecStart=/usr/bin/hysteria\n", unmanaged.read_text())
        systemctl = (self.remote / "systemctl.log").read_text()
        self.assertNotIn("restart hysteria", systemctl)
        self.assertNotIn("stop hysteria", systemctl)

    def test_existing_german_m1_and_v1_are_adopted_not_duplicated(self):
        old = self.remote / "etc/olcrtc"
        old.mkdir(parents=True)
        m1 = "mode: srv\nroom:\n  id: adopted-m1\ncrypto:\n  key: adopted-m1-key\n"
        v1 = "mode: srv\nroom:\n  id: adopted-v1\ncrypto:\n  key: adopted-v1-key\n"
        (old / "m1.yaml").write_text(m1)
        (old / "v1.yaml").write_text(v1)
        self.build()
        result = self.deploy()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        managed = self.remote / "etc/ilyukhaline"
        egress = "egress:\n  allow: '127.0.0.1:10000'\n"
        self.assertEqual(m1 + egress, (managed / "m1.yaml").read_text())
        self.assertEqual(v1 + egress, (managed / "v1.yaml").read_text())
        self.assertFalse((managed / "m1-extra.yaml").exists())
        self.assertFalse((managed / "v1-extra.yaml").exists())
        systemctl = (self.remote / "systemctl.log").read_text()
        self.assertIn("disable olcrtc@m1.service olcrtc@v1.service", systemctl)
        self.assertIn("stop olcrtc@m1.service olcrtc@v1.service", systemctl)

    def seed_real_german_layout(self):
        old = self.remote / "etc/olcrtc"
        old.mkdir(parents=True)
        for index in range(1, 5):
            (old / f"server-mamba-{index}.yaml").write_text(f"mode: srv\nroom:\n  id: adopted-m{index}\n")
        (old / "server-vkcalls.yaml").write_text("mode: srv\nroom:\n  id: adopted-v1\n")
        xray = self.remote / "usr/local/etc/xray/config.json"
        xray.parent.mkdir(parents=True)
        xray.write_text(
            '{"inbounds":[{"listen":"0.0.0.0","port":10000,"protocol":"vless",'
            '"settings":{"clients":[{"id":"11111111-1111-1111-1111-111111111111"}]}}]}'
        )
        (self.remote / "active-units").write_text(
            "olcrtc@1.service\nolcrtc@2.service\nolcrtc@3.service\nolcrtc@4.service\n"
            "olcrtc-vkcalls.service\nxray.service\n"
        )

    def test_real_german_layout_is_adopted_without_changing_rooms(self):
        self.seed_real_german_layout()
        self.build()
        result = self.deploy()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        managed = self.remote / "etc/ilyukhaline"
        for index in range(1, 5):
            self.assertIn(f"adopted-m{index}", (managed / f"m{index}.yaml").read_text())
        self.assertIn("adopted-v1", (managed / "v1.yaml").read_text())
        active = (self.remote / "active-units").read_text()
        self.assertIn("ilyukhaline-xray.service", active)
        self.assertNotIn("olcrtc@1.service", active)
        self.assertNotIn("olcrtc-vkcalls.service", active)
        self.assertNotIn("xray.service\n", active.replace("ilyukhaline-xray.service\n", ""))

    def test_failed_real_german_deploy_restores_old_units(self):
        self.seed_real_german_layout()
        self.build()
        active = (self.remote / "active-units").read_text().splitlines()
        result = self.deploy(env={"FAKE_XRAY_TEST_FAIL": "1"})
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(set(active), set((self.remote / "active-units").read_text().splitlines()))
        self.assertFalse((self.remote / "etc/ilyukhaline").exists())

    def test_invalid_xray_restores_previous_files_and_units(self):
        self.build()
        first = self.deploy()
        self.assertEqual(0, first.returncode, first.stdout + first.stderr)
        snapshot = self.managed_snapshot()
        active = (self.remote / "active-units").read_bytes()
        failed = self.deploy(env={"FAKE_XRAY_TEST_FAIL": "1"})
        self.assertNotEqual(0, failed.returncode)
        self.assertEqual(snapshot, self.managed_snapshot())
        self.assertEqual(active, (self.remote / "active-units").read_bytes())
        systemctl = (self.remote / "systemctl.log").read_text()
        self.assertNotIn("hysteria", systemctl)

    def test_status_reports_labels_and_health_without_secrets(self):
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        result = self.run_script("--config", str(self.config), "status")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        for label in ("M1", "M4", "V1", "V4", "T1", "T4"):
            self.assertIn(label, result.stdout)
        self.assertIn("127.0.0.1:10000", result.stdout)
        self.assertIn("active", result.stdout)
        values = self.values()
        secrets = [values["VLESS_UUID"]]
        secrets.extend(value for key, value in values.items() if "_KEY_" in key or "_URL_" in key)
        for secret in secrets:
            self.assertNotIn(secret, result.stdout + result.stderr)
    def test_dead_room_service_rolls_back_the_deployment(self):
        self.build()
        result = self.deploy(env={"FAKE_INACTIVE_UNIT": "ilyukhaline-olcrtc@m1.service"})
        self.assertNotEqual(0, result.returncode)
        self.assertFalse((self.remote / "etc/ilyukhaline").exists())

    def test_observer_timeout_never_kills_the_remote_transaction(self):
        self.build()
        runner = self.remote / "remote-bin/systemd-run"
        runner.write_text(
            "#!/usr/bin/env python3\n"
            "import os,sys\n"
            "from pathlib import Path\n"
            "root=Path(os.environ['FAKE_REMOTE_ROOT'])\n"
            "unit=next(v.split('=',1)[1] for v in sys.argv if v.startswith('--unit='))\n"
            "(root / f'{unit}.status').write_text('ActiveState=active\\n')\n"
        )
        runner.chmod(0o755)
        sleeper = self.bin / "sleep"
        sleeper.write_text("#!/usr/bin/env sh\nexit 0\n")
        sleeper.chmod(0o755)

        result = self.deploy()

        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("stop ilyukhaline-deploy", (self.remote / "systemctl.log").read_text())

    def test_multi_inbound_xray_is_not_adopted(self):
        self.seed_real_german_layout()
        import json
        xray = self.remote / "usr/local/etc/xray/config.json"
        config = json.loads(xray.read_text())
        config["inbounds"].append({"protocol": "socks", "listen": "127.0.0.1", "port": 1080, "settings": {}})
        xray.write_text(json.dumps(config))
        before = xray.read_bytes()
        active = set((self.remote / "active-units").read_text().splitlines())
        self.build()

        result = self.deploy()

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(before, xray.read_bytes())
        self.assertEqual(active, set((self.remote / "active-units").read_text().splitlines()))
        self.assertFalse((self.remote / "etc/ilyukhaline").exists())

    def test_disabling_telemost_removes_only_its_managed_rooms(self):
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        managed = self.remote / "etc/ilyukhaline"
        m1 = (managed / "m1.yaml").read_bytes()
        v1 = (managed / "v1.yaml").read_bytes()
        self.write_config({"TELEMOST_ENABLED": "false"})
        systemctl_log = self.remote / "systemctl.log"
        systemctl_log.write_text("")

        self.assertEqual(0, self.deploy().returncode)

        self.assertEqual(m1, (managed / "m1.yaml").read_bytes())
        self.assertEqual(v1, (managed / "v1.yaml").read_bytes())
        self.assertNotIn("restart ilyukhaline-olcrtc@m1.service", systemctl_log.read_text())
        self.assertNotIn("restart ilyukhaline-olcrtc@v1.service", systemctl_log.read_text())
        self.assertNotIn("restart ilyukhaline-xray.service", systemctl_log.read_text())
        self.assertFalse((managed / "t1.yaml").exists())
        self.assertFalse((managed / "t4.yaml").exists())
        active = (self.remote / "active-units").read_text()
        self.assertNotIn("ilyukhaline-olcrtc@t1.service", active)
        self.assertNotIn("ilyukhaline-olcrtc@t4.service", active)
        self.assertEqual(9, len(active.splitlines()))

    def test_salutejazz_deploys_four_datachannel_rooms_idempotently(self):
        rooms = self.enable_salutejazz()
        self.build()
        result = self.deploy()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        managed = self.remote / "etc/ilyukhaline"
        for index in range(1, 5):
            text = (managed / f"j{index}.yaml").read_text()
            self.assertIn("provider: salutejazz", text)
            self.assertIn("transport: datachannel", text)
            self.assertIn(f"id: 'room{index}:pass{index}word'", text)
            self.assertIn("egress:\n  allow: '127.0.0.1:10000'", text)
            self.assertIn(f"ilyukhaline-olcrtc@j{index}.service", (self.remote / "active-units").read_text())
        for secret in (self.salutejazz_rooms_text(), "04" * 32):
            self.assertNotIn(secret, result.stdout + result.stderr)
        status = self.run_script("--config", str(self.config), "status")
        self.assertIn("J1 salutejazz active", status.stdout)
        self.assertIn("J4 salutejazz active", status.stdout)
        self.assertNotIn(rooms.read_text(), status.stdout + status.stderr)
        snapshot = self.managed_snapshot()
        self.assertEqual(0, self.deploy().returncode)
        self.assertEqual(snapshot, self.managed_snapshot())

    def test_salutejazz_rejects_incomplete_duplicate_and_mismatched_groups(self):
        for content in (
            self.salutejazz_rooms_text(partial=True),
            self.salutejazz_rooms_text(duplicate=True),
            self.salutejazz_rooms_text(mismatched_key=True),
            self.salutejazz_rooms_text().replace("room1:pass1word", "ROOM1:pass1word"),
        ):
            with self.subTest(content=content):
                self.enable_salutejazz(content)
                result = self.run_script("--config", str(self.config), "check")
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("pass1word", result.stdout + result.stderr)

    def test_additive_salutejazz_keeps_existing_cores_running(self):
        self.write_config({"TELEMOST_ENABLED": "false"})
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        managed = self.remote / "etc/ilyukhaline"
        before = {name: (managed / name).read_bytes() for name in ("m1.yaml", "v1.yaml", "xray.json")}
        systemctl_log = self.remote / "systemctl.log"
        systemctl_log.write_text("")
        self.enable_salutejazz()

        self.assertEqual(0, self.deploy().returncode)

        self.assertEqual(before, {name: (managed / name).read_bytes() for name in before})
        log = systemctl_log.read_text()
        for unit in ("ilyukhaline-olcrtc@m1.service", "ilyukhaline-olcrtc@v1.service", "ilyukhaline-xray.service"):
            self.assertNotIn(f"restart {unit}", log)
        self.assertIn("restart ilyukhaline-olcrtc@j1.service", log)
        self.assertIn("restart ilyukhaline-olcrtc@j4.service", log)

    def test_room_outage_restarts_only_the_selected_room_after_interruption(self):
        self.enable_salutejazz()
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        systemctl_log = self.remote / "systemctl.log"
        systemctl_log.write_text("")
        sleeper = self.bin / "sleep"
        sleeper.write_text("#!/usr/bin/env sh\nexit 1\n")
        sleeper.chmod(0o755)

        result = self.run_script("--config", str(self.config), "room-outage", "--label", "j2", "--seconds", "1")

        self.assertNotEqual(0, result.returncode)
        log = systemctl_log.read_text()
        self.assertIn("stop ilyukhaline-olcrtc@j2.service", log)
        self.assertIn("restart ilyukhaline-olcrtc@j2.service", log)
        self.assertNotIn("restart ilyukhaline-olcrtc@j1.service", log)
        self.assertNotIn("restart ilyukhaline-olcrtc@m1.service", log)
        invalid = self.run_script("--config", str(self.config), "room-outage", "--label", "m1", "--seconds", "1")
        self.assertNotEqual(0, invalid.returncode)


class SubscriptionTest(DeployLifecycleTest):
    def prepare_second_vds(self, telemost=True, salutejazz=False):
        changes = {
            "SSH_TARGET": "root@192.0.2.20",
            "ARTIFACT_DIR": str(self.root / "artifacts-fi"),
            "PUBLISH_PATH": "/var/www/ilyukhaline/finland.txt",
            "PUBLISH_URL": "https://finland.example.invalid/finland.txt",
            "VLESS_UUID": "22222222-2222-2222-2222-222222222222",
            "VLESS_LISTEN": "127.0.0.1:10001",
            "TELEMOST_ENABLED": "true" if telemost else "false",
        }
        if salutejazz:
            rooms = self.root / "finland-salutejazz-rooms.conf"
            rooms.write_text(self.salutejazz_rooms_text().replace("room", "firoom"))
            rooms.chmod(0o600)
            changes["SALUTEJAZZ_ENABLED"] = "true"
            changes["SALUTEJAZZ_ROOMS_FILE"] = str(rooms)
        for provider in ("MAMBA", "VKCALLS", "TELEMOST"):
            for index in range(1, 5):
                key = f"{provider}_URL_{index}"
                changes[key] = self.values()[key] + "-fi"
        self.second_config = self.root / "finland.conf"
        self.second_config.write_text(self.config_text(changes))
        self.remote2 = self.root / "remote-fi"
        remote_bin = self.remote2 / "remote-bin"
        remote_bin.mkdir(parents=True)
        systemctl = remote_bin / "systemctl"
        systemctl.write_text(FAKE_SYSTEMCTL)
        systemctl.chmod(0o755)
        self.install_fake_nginx(self.remote2)
        self.env["FAKE_REMOTE_ROOT_2"] = str(self.remote2)
        for command in ("build", "deploy"):
            result = self.run_script("--config", str(self.second_config), command)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def generate(self):
        self.enable_salutejazz()
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        self.prepare_second_vds(telemost=False, salutejazz=True)
        (self.remote / "etc/ilyukhaline/unmanaged.yaml").write_text("room: unmanaged\n")
        self.output = self.root / "subscription.txt"
        result = self.run_script(
            "--config", str(self.config),
            "--config", str(self.second_config),
            "subscription", "--output", str(self.output),
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertNotIn("olcrtc://", result.stdout + result.stderr)
        self.assertIn("generated 6 profiles", result.stdout)
        return self.output.read_text()

    def test_subscription_has_exact_groups_profiles_paths_and_snapshot(self):
        body = self.generate()
        primary = [line for line in body.splitlines() if line.startswith("olcrtc://")]
        self.assertEqual(6, len(primary))
        expected_names = [
            f"{group} — {profile}"
            for group in ("Германия", "Финляндия")
            for profile in (
                "Mamba 4x (пойдет для мобильного интернета)",
                "VK 4x (обход белых списков)",
                "SaluteJazz 4x",
            )
        ]
        self.assertEqual(expected_names, [line.split("$", 1)[1] for line in primary])
        self.assertEqual(6, body.count("##group: "))
        self.assertEqual(6, body.count("##rooms: "))
        self.assertEqual(0, body.count("##bond-room: "))
        for unwanted in ("Multi", "telemost", "mtslink"):
            self.assertNotIn(unwanted, body)
        self.assertEqual(0o600, self.output.stat().st_mode & 0o777)
        self.assertNotIn("unmanaged", body)
        self.assertEqual(2, sum("salutejazz?datachannel" in line for line in primary))
        for values in (self.values(), self.values()):
            for key, value in values.items():
                if "_URL_" in key and not key.startswith("TELEMOST"):
                    self.assertEqual(2, body.count(value))
        for key in ("MAMBA_URL_1", "VKCALLS_URL_1"):
            value = self.values()[key] + "-fi"
            self.assertEqual(1, body.count(f"@{value}#"))
        expected = HERE.parent / "deploy/testdata/subscription.expected.txt"
        self.assertEqual(expected.read_text(), body)

    def test_mixed_telemost_availability_still_omits_multi_profiles(self):
        self.build()
        self.assertEqual(0, self.deploy().returncode)
        self.prepare_second_vds(telemost=False)
        self.output = self.root / "subscription.txt"
        result = self.run_script(
            "--config", str(self.config),
            "--config", str(self.second_config),
            "subscription", "--output", str(self.output),
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        primary = [line for line in self.output.read_text().splitlines() if line.startswith("olcrtc://")]
        expected_names = [
            "Германия — Mamba 4x (пойдет для мобильного интернета)",
            "Германия — VK 4x (обход белых списков)",
            "Германия — Телемост 4x",
            "Финляндия — Mamba 4x (пойдет для мобильного интернета)",
            "Финляндия — VK 4x (обход белых списков)",
        ]
        self.assertEqual(expected_names, [line.split("$", 1)[1] for line in primary])
        self.assertIn("generated 5 profiles", result.stdout)
        self.assertNotIn("Multi", self.output.read_text())
        self.assertFalse((self.remote2 / "etc/ilyukhaline/t1.yaml").exists())
        active = (self.remote2 / "active-units").read_text()
        self.assertNotIn("ilyukhaline-olcrtc@t1.service", active)
        self.assertEqual(9, len(active.splitlines()))

    def test_mamba_pool_size_eight_and_twelve(self):
        for count in (8, 12):
            with self.subTest(count=count):
                self.setUp()
                changes = {"TELEMOST_ENABLED": "false", "MAMBA_COUNT": str(count)}
                for index in range(5, count + 1):
                    changes[f"MAMBA_URL_{index}"] = f"https://mamba.example.invalid/room-{index}"
                    changes[f"MAMBA_KEY_{index}"] = "01" * 32
                self.write_config(changes)
                build = self.run_script("--config", str(self.config), "build")
                self.assertEqual(0, build.returncode, build.stdout + build.stderr)
                result = self.run_script("--config", str(self.config), "deploy")
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                active = (self.remote / "active-units").read_text()
                for index in range(1, count + 1):
                    self.assertIn(f"ilyukhaline-olcrtc@m{index}.service", active)
                self.assertNotIn(f"ilyukhaline-olcrtc@m{count + 1}.service", active)
                status = self.run_script("--config", str(self.config), "status")
                self.assertIn(f"M{count} mamba active", status.stdout)

        self.write_config({"MAMBA_COUNT": "5", "MAMBA_URL_5": "https://mamba.example.invalid/room-5", "MAMBA_KEY_5": "01" * 32})
        result = self.run_script("--config", str(self.config), "check")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("MAMBA_COUNT", result.stderr)

    def test_mamba_generator_supports_eight_and_twelve_rooms(self):
        spec = importlib.util.spec_from_file_location(
            "subscription_generator", HERE.parent / "deploy/generate_subscription.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        import json
        for count in (8, 12):
            with self.subTest(count=count):
                sections = ["###XRAY", json.dumps({"inbounds": [{"listen": "127.0.0.1", "port": 10000, "settings": {"clients": [{"id": "1" * 32}]}}]})]
                for prefix, provider, transport, size in (("m", "jitsi", "datachannel", count), ("v", "vkcalls", "vp8channel", 4)):
                    for index in range(1, size + 1):
                        sections.extend([
                            f"###ROOM {prefix}{index}",
                            f"auth:\n  provider: {provider}\nroom:\n  id: room{index}\ncrypto:\n  key: {'04' * 32}\nnet:\n  transport: {transport}\n",
                        ])
                bundle = self.root / f"mamba-{count}.bundle"
                bundle.write_text("\n".join(sections))
                rooms, vless = module.load_bundle(bundle)
                lines = module.render_group("Германия", rooms, vless)
                self.assertTrue(lines[0].endswith(f"Германия — Mamba {count}x (пойдет для мобильного интернета)"))
                self.assertEqual(count - 1, lines[2].count(",") + 1)

    def test_subscription_fields_strip_supported_yaml_quotes(self):
        spec = importlib.util.spec_from_file_location(
            "subscription_generator", HERE.parent / "deploy/generate_subscription.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for value in ("value", "'value'", '"value"'):
            with self.subTest(value=value):
                self.assertEqual("value", module.field(f"crypto:\n  key: {value}\n", "key"))

    def test_adopted_homogeneous_keys_use_full_links(self):
        spec = importlib.util.spec_from_file_location(
            "subscription_generator", HERE.parent / "deploy/generate_subscription.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        rooms = {}
        providers = {"m": "jitsi", "v": "vkcalls", "t": "telemost", "j": "salutejazz"}
        for label in module.LABELS:
            rooms[label] = {
                "provider": providers[label[0]],
                "room": label,
                "key": "a" * 64,
                "transport": "datachannel" if label[0] in ("m", "j") else "vp8channel",
            }
        rooms["v1"]["key"] = "b" * 64
        vless = {"host": "127.0.0.1", "port": 10000, "id": "11111111-1111-1111-1111-111111111111"}

        lines = module.render_group("Германия", rooms, vless)
        start = next(index for index, line in enumerate(lines) if line.endswith("$Германия — VK 4x (обход белых списков)"))
        end = next(index for index, line in enumerate(lines) if line.endswith("$Германия — Телемост 4x"))
        vk = lines[start:end]
        self.assertFalse(any(line.startswith("##rooms:") for line in vk))
        self.assertEqual(3, sum(line.startswith("##bond-room:") for line in vk))
        self.assertIn("#" + "b" * 64, vk[0])

    def test_salutejazz_generator_rejects_partial_groups_and_preserves_adopted_keys(self):
        spec = importlib.util.spec_from_file_location(
            "subscription_generator", HERE.parent / "deploy/generate_subscription.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        import json
        sections = ["###XRAY", json.dumps({"inbounds": [{"listen": "127.0.0.1", "port": 10000, "settings": {"clients": [{"id": "1" * 32}]}}]})]
        for prefix, provider, transport, count in (
            ("m", "jitsi", "datachannel", 4),
            ("v", "vkcalls", "vp8channel", 4),
            ("j", "salutejazz", "datachannel", 3),
        ):
            for index in range(1, count + 1):
                sections.extend([
                    f"###ROOM {prefix}{index}",
                    f"auth:\n  provider: {provider}\nroom:\n  id: room{index}\ncrypto:\n  key: {'04' * 32}\nnet:\n  transport: {transport}\n",
                ])
        bundle = self.root / "partial.bundle"
        bundle.write_text("\n".join(sections))
        with self.assertRaisesRegex(ValueError, "SaluteJazz"):
            module.load_bundle(bundle)

        rooms = {}
        for prefix, provider, transport in (("m", "jitsi", "datachannel"), ("v", "vkcalls", "vp8channel"), ("j", "salutejazz", "datachannel")):
            for index in range(1, 5):
                rooms[f"{prefix}{index}"] = {"provider": provider, "room": f"room{index}", "key": "a" * 64, "transport": transport}
        rooms["j1"]["key"] = "b" * 64
        vless = {"host": "127.0.0.1", "port": 10000, "id": "1" * 32}
        lines = module.render_group("Германия", rooms, vless)
        start = next(index for index, line in enumerate(lines) if line.endswith("$Германия — SaluteJazz 4x"))
        jazz = lines[start:]
        self.assertEqual(3, sum(line.startswith("##bond-room:") for line in jazz))
        self.assertFalse(any(line.startswith("##rooms:") for line in jazz))
        self.assertIn("#" + "b" * 64, jazz[0])

    def test_publish_installs_only_private_file_and_nginx_location(self):
        body = self.generate()
        self.write_config({"PUBLISH_SSH_TARGET": "root@192.0.2.20"})
        nginx = self.remote2 / "etc/nginx/sites-enabled/default"
        nginx.parent.mkdir(parents=True, exist_ok=True)
        nginx.write_text("server {\n  listen 443 ssl;\n  server_name example.invalid;\n}\n")
        result = self.run_script("--config", str(self.config), "publish", "--input", str(self.output))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        published = self.remote2 / "var/www/ilyukhaline/private.txt"
        self.assertEqual(body, published.read_text())
        self.assertEqual(0o640, published.stat().st_mode & 0o777)
        self.assertEqual(
            f"www-data {published}.new",
            (self.remote2 / "chgrp.log").read_text().strip(),
        )
        snippet = self.remote2 / "etc/nginx/snippets/ilyukhaline-subscription.conf"
        self.assertIn("autoindex off;", snippet.read_text())
        self.assertIn("location = /private.txt", snippet.read_text())
        self.assertIn(f"include {self.remote2}/etc/nginx/snippets/ilyukhaline-subscription.conf;", nginx.read_text())
        self.assertNotIn(self.values()["VLESS_UUID"], result.stdout + result.stderr)
        self.assertNotIn(body, result.stdout + result.stderr)

    def test_publish_creates_dedicated_vhost_when_missing(self):
        self.generate()
        self.write_config({"PUBLISH_SSH_TARGET": "root@192.0.2.20"})
        result = self.run_script("--config", str(self.config), "publish", "--input", str(self.output))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        nginx = self.remote2 / "etc/nginx/sites-enabled/default"
        self.assertIn("server_name example.invalid;", nginx.read_text())
        self.assertIn("include", nginx.read_text())

    def test_publish_restores_live_files_when_a_write_fails(self):
        self.generate()
        self.write_config({"PUBLISH_SSH_TARGET": "root@192.0.2.20"})
        result = self.run_script("--config", str(self.config), "publish", "--input", str(self.output))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        published = self.remote2 / "var/www/ilyukhaline/private.txt"
        snippet = self.remote2 / "etc/nginx/snippets/ilyukhaline-subscription.conf"
        server = self.remote2 / "etc/nginx/sites-enabled/default"
        before = (published.read_bytes(), snippet.read_bytes(), server.read_bytes())
        installer = self.remote2 / "remote-bin/install"
        installer.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$*\" == *ilyukhaline-subscription.conf* ]]; then exit 1; fi\n"
            "exec /usr/bin/install \"$@\"\n"
        )
        installer.chmod(0o755)
        self.output.write_text("replacement")

        result = self.run_script("--config", str(self.config), "publish", "--input", str(self.output))

        self.assertNotEqual(0, result.returncode)
        self.assertEqual(before, (published.read_bytes(), snippet.read_bytes(), server.read_bytes()))

    def test_publish_rejects_missing_input_and_unescaped_path(self):
        missing = self.run_script("--config", str(self.config), "publish", "--input", str(self.root / "missing"))
        self.assertNotEqual(0, missing.returncode)
        self.write_config({"PUBLISH_PATH": "/var/www/ilyukhaline/../private.txt"})
        result = self.run_script("--config", str(self.config), "check")
        self.assertNotEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main()

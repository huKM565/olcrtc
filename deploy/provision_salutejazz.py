#!/usr/bin/env python3
import argparse
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path

ROOM_RE = re.compile(r"^[a-z0-9]{4,16}:[a-z0-9]{4,16}$")
KEY_RE = re.compile(r"^[0-9a-f]{64}$")
ASSIGNMENT_RE = re.compile(r"SALUTEJAZZ_(URL|KEY)_([1-4])='([^']*)'")


class ProvisionError(Exception):
    pass


def load_existing(output):
    values = {}
    if not output.exists():
        return values
    for line in output.read_text().splitlines():
        match = ASSIGNMENT_RE.fullmatch(line)
        if match is None:
            raise ProvisionError("malformed existing room file")
        name = f"SALUTEJAZZ_{match.group(1)}_{match.group(2)}"
        if name in values:
            raise ProvisionError("duplicate assignment in existing room file")
        values[name] = match.group(3)
    rooms = []
    keys = []
    for index in range(1, 5):
        room = values.get(f"SALUTEJAZZ_URL_{index}")
        key = values.get(f"SALUTEJAZZ_KEY_{index}")
        if (room is None) != (key is None):
            raise ProvisionError("incomplete room assignment")
        if room is not None:
            if ROOM_RE.fullmatch(room) is None or KEY_RE.fullmatch(key) is None:
                raise ProvisionError("invalid value in existing room file")
            rooms.append(room)
            keys.append(key)
    if len(set(rooms)) != len(rooms):
        raise ProvisionError("duplicate room in existing room file")
    if len(set(keys)) > 1:
        raise ProvisionError("existing room keys do not match")
    return values


def create_room(tool):
    try:
        result = subprocess.run(
            [str(tool)],
            text=True,
            capture_output=True,
            timeout=40,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise ProvisionError("room provisioner timed out") from error
    if result.returncode != 0:
        raise ProvisionError("room provisioner failed")
    room = result.stdout.strip()
    if ROOM_RE.fullmatch(room) is None:
        raise ProvisionError("room provisioner returned an invalid room")
    return room


def render(values, key):
    lines = []
    for index in range(1, 5):
        lines.append(f"SALUTEJAZZ_URL_{index}='{values[f'SALUTEJAZZ_URL_{index}']}'")
        lines.append(f"SALUTEJAZZ_KEY_{index}='{key}'")
    return "\n".join(lines) + "\n"


def atomic_write(output, text):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}-{secrets.token_hex(4)}")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def provision(tool, output):
    tool = Path(tool)
    output = Path(output)
    if not tool.is_absolute() or not tool.is_file() or not os.access(tool, os.X_OK):
        raise ProvisionError("room tool must be an absolute executable path")
    if not output.is_absolute():
        raise ProvisionError("room output must be an absolute path")
    values = load_existing(output)
    existing_rooms = [values.get(f"SALUTEJAZZ_URL_{index}") for index in range(1, 5)]
    if all(existing_rooms):
        output.chmod(0o600)
        return 0
    key = next((values[f"SALUTEJAZZ_KEY_{index}"] for index in range(1, 5) if f"SALUTEJAZZ_KEY_{index}" in values), None)
    key = key or secrets.token_hex(32)
    rooms = list(existing_rooms)
    created = 0
    for index in range(4):
        if rooms[index] is not None:
            continue
        room = create_room(tool)
        if room in rooms:
            raise ProvisionError("room provisioner returned a duplicate room")
        rooms[index] = room
        values[f"SALUTEJAZZ_URL_{index + 1}"] = room
        created += 1
    atomic_write(output, render(values, key))
    return created


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        created = provision(args.tool, args.output)
    except (ProvisionError, OSError) as error:
        print(f"provision failed: {error}", file=sys.stderr)
        return 1
    print(f"provisioned {created} SaluteJazz rooms; 4 rooms available")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
import argparse
import json
import os
import re
from pathlib import Path

LABELS = tuple(
    [f"m{index}" for index in range(1, 13)]
    + [f"{provider}{index}" for provider in "vtj" for index in range(1, 5)]
)
PROFILES = (
    ("Mamba", "m"),
    ("VK 4x (обход белых списков)", "v"),
    ("Телемост 4x", "t"),
    ("SaluteJazz 4x", "j"),
)


def field(text, name):
    match = re.search(rf"(?m)^  {re.escape(name)}: (.*)$", text)
    if not match:
        raise ValueError(f"managed config has no {name}")
    value = match.group(1).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        value = value[1:-1]
    return value


def load_bundle(path):
    text = Path(path).read_text()
    parts = re.split(r"(?m)^###(XRAY|ROOM (?:m(?:[1-9]|1[0-2])|[vtj][1-4]))\n", text)
    sections = {}
    for index in range(1, len(parts), 2):
        sections[parts[index]] = parts[index + 1]
    if "XRAY" not in sections:
        raise ValueError("managed bundle has no Xray config")
    xray = json.loads(sections["XRAY"])
    inbound = xray["inbounds"][0]
    vless = {
        "host": inbound["listen"],
        "port": inbound["port"],
        "id": inbound["settings"]["clients"][0]["id"],
    }
    rooms = {}
    for label in LABELS:
        config = sections.get(f"ROOM {label}")
        if config is None:
            if label[0] == "v" or (label[0] == "m" and int(label[1:]) <= 4):
                raise ValueError(f"managed bundle has no room {label}")
            continue
        room = {
            "provider": field(config, "provider"),
            "room": field(config, "id"),
            "key": field(config, "key"),
            "transport": field(config, "transport"),
        }
        if any(char in room["room"] for char in "\r\n#"):
            raise ValueError(f"room {label} contains an unsafe character")
        rooms[label] = room
    mamba_count = sum(label.startswith("m") for label in rooms)
    if mamba_count not in (4, 8, 12):
        raise ValueError("managed bundle has an incomplete Mamba group")
    for prefix, name in (("t", "Telemost"), ("j", "SaluteJazz")):
        count = sum(label.startswith(prefix) for label in rooms)
        if count not in (0, 4):
            raise ValueError(f"managed bundle has an incomplete {name} group")
    return rooms, vless


def olc_link(room, name=None):
    link = f"olcrtc://{room['provider']}?{room['transport']}@{room['room']}#{room['key']}"
    return link if name is None else f"{link}${name}"


def vless_link(vless):
    host = vless["host"]
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"vless://{vless['id']}@{host}:{vless['port']}?security=none&encryption=none#VLESS"


def render_group(group, rooms, vless):
    output = []
    bond_vless = vless_link(vless)
    for profile, prefix in PROFILES:
        if f"{prefix}1" not in rooms:
            continue
        available = sum(label.startswith(prefix) for label in rooms) if prefix == "m" else 4
        counts = (4, 8, 12) if prefix == "m" and available == 12 else (available,)
        for count in counts:
            selected = [rooms[f"{prefix}{index}"] for index in range(1, count + 1)]
            keys = {room["key"] for room in selected}
            profile_name = f"{profile} {count}x (пойдет для мобильного интернета)" if prefix == "m" else profile
            output.append(olc_link(selected[0], f"{group} — {profile_name}"))
            output.append(f"##group: {group}")
            if len(keys) == 1:
                output.append("##rooms: " + ", ".join(room["room"] for room in selected[1:]))
            else:
                output.extend(f"##bond-room: {olc_link(room)}" for room in selected[1:])
            output.append(f"##bond-vless: {bond_vless}")
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vds", action="append", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= len(args.vds) <= 2:
        raise ValueError("one or two VDS bundles are required")
    lines = []
    for item in args.vds:
        group, path = item.split("=", 1)
        if group not in ("Германия", "Финляндия"):
            raise ValueError("unexpected VDS group")
        rooms, vless = load_bundle(path)
        lines.extend(render_group(group, rooms, vless))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    temporary.write_text("\n".join(lines) + "\n")
    temporary.chmod(0o600)
    temporary.replace(output)


if __name__ == "__main__":
    main()

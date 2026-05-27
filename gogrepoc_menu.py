#!/usr/bin/env python3
"""
Interactive menu wrapper for gogrepoc.py.
"""

from __future__ import annotations

import subprocess
import sys
import shlex
from pathlib import Path

import gogrepoc
from modules.config import get_user_paths
from modules.manifest import load_manifest


SCRIPT_PATH = Path(__file__).with_name("gogrepoc.py")


def print_help_text(help_text: str) -> None:
    for line in help_text.splitlines():
        print(f"> {line}")


def prompt_text(label: str, default: str = "", help_text: str | None = None) -> str:
    while True:
        prompt = f"{label}"
        if default:
            prompt += f" [{default}]"
        prompt += ": "
        value = input(prompt).strip()
        if value.lower() in {"?", "help"} and help_text:
            print_help_text(help_text)
            continue
        return value or default


def prompt_required_text(label: str, help_text: str | None = None) -> str:
    while True:
        value = prompt_text(label, "", help_text=help_text)
        if value:
            return value
        print("A value is required.")


def prompt_bool(label: str, default: bool = False, help_text: str | None = None) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        value = input(f"{label} [{suffix}]: ").strip().lower()
        if not value:
            return default
        if value in {"?", "help"}:
            if help_text:
                print_help_text(help_text)
            else:
                print_help_text("Enter y for yes or n for no.")
            continue
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Enter y or n.")


def prompt_choice(
    label: str,
    options: list[str],
    default: str,
    help_text: dict[str, str] | str | None = None,
) -> str:
    display = ", ".join(options)
    while True:
        value = prompt_text(f"{label} ({display})", default).lower()
        if value in {"?", "help"}:
            if isinstance(help_text, dict):
                for option in options:
                    description = help_text.get(option)
                    if description:
                        print_help_text(f"{option}: {description}")
            elif help_text:
                print_help_text(help_text)
            else:
                print_help_text(f"Choose one of: {display}")
            continue
        if value in options:
            return value
        print(f"Choose one of: {display}")


def prompt_execution_mode(supports_dryrun: bool) -> str:
    if not supports_dryrun:
        return "run"
    return prompt_choice(
        "Execution mode",
        ["run", "preview", "preview_then_run"],
        "run",
        {
            "run": "Perform the action immediately.",
            "preview": "Show what would happen without changing files.",
            "preview_then_run": "Show the preview first, then ask whether to run for real.",
        },
    )


def prompt_show_advanced(topic: str = "for this command") -> bool:
    return prompt_bool(
        f"Show advanced options {topic}",
        False,
        "Advanced options expose less common flags for troubleshooting, recovery, or fine-grained control.",
    )


def parse_list(raw: str) -> list[str]:
    if not raw.strip():
        return []
    normalized = raw.replace(",", " ")
    return [item for item in shlex.split(normalized) if item]


def prompt_list(label: str, default: str = "", help_text: str | None = None) -> list[str]:
    return parse_list(prompt_text(label, default, help_text=help_text))


def add_multi_value_args(args: list[str], flag: str, values: list[str]) -> None:
    if values:
        args.append(flag)
        args.extend(values)


def session_args(session: dict[str, str | bool]) -> list[str]:
    args: list[str] = []
    user = str(session["user"])
    if user:
        args.extend(["--user", user])
    if bool(session["nolog"]):
        args.append("-nolog")
    if bool(session["debug"]):
        args.append("-debug")
    return args


def prompt_user_profile() -> str:
    return prompt_text(
        "User profile",
        "",
        "Leave blank to use the default account profile. Enter a short name to use a separate user profile.",
    )


def prompt_session_settings(current: dict[str, str | bool] | None = None) -> dict[str, str | bool]:
    current = current or {"user": "", "nolog": False, "debug": False}
    session = {
        "user": prompt_text(
            "User profile",
            str(current["user"]),
            "Leave blank to use the default account profile. Enter a short name to use a separate user profile.",
        ),
        "nolog": bool(current["nolog"]),
        "debug": bool(current["debug"]),
    }
    if prompt_show_advanced("for global settings"):
        session["nolog"] = prompt_bool(
            "Disable logfile output",
            bool(current["nolog"]),
            "Do not write gogrepo.log. Useful if you only want on-screen output.",
        )
        session["debug"] = prompt_bool(
            "Enable debug output",
            bool(current["debug"]),
            "Show more detailed diagnostic output. Useful when troubleshooting.",
        )
    return session


def prompt_filters(args: list[str], include_ids: bool = True) -> None:
    if not prompt_bool(
        "Apply filters to this run",
        False,
        "Filters let you limit the command to specific games, operating systems, or languages instead of processing everything.",
    ):
        return

    if include_ids and prompt_bool(
        "Filter by specific games",
        False,
        "Use game slugs, titles, or numeric GOG IDs from your manifest to include or exclude particular games.",
    ):
        add_multi_value_args(
            args,
            "-ids",
            prompt_list(
                "Specific games to include",
                "",
                "Enter one or more game slugs, titles, or numeric GOG IDs separated by spaces or commas. Leave blank to include all games.",
            ),
        )
        add_multi_value_args(
            args,
            "-skipids",
            prompt_list(
                "Specific games to exclude",
                "",
                "Enter game slugs, titles, or numeric GOG IDs to exclude, separated by spaces or commas. Leave blank to exclude none.",
            ),
        )

    if prompt_bool(
        "Filter by operating system",
        False,
        "Limit the command to specific platforms such as windows, mac, or linux.",
    ):
        os_values = prompt_list(
            f"Limit to operating systems ({', '.join(gogrepoc.VALID_OS_TYPES)})",
            "",
            "Choose one or more operating systems separated by spaces or commas. Leave blank to use the command default.",
        )
        add_multi_value_args(args, "-os", os_values)

    if prompt_bool(
        "Filter by language",
        False,
        "Limit the command to specific language variants such as en or de.",
    ):
        lang_values = prompt_list(
            f"Limit to languages ({', '.join(gogrepoc.VALID_LANG_TYPES)})",
            "",
            "Choose one or more language codes separated by spaces or commas, such as en or de. Leave blank to use the command default.",
        )
        add_multi_value_args(args, "-lang", lang_values)


def build_login_args(session: dict[str, str | bool]) -> list[str]:
    return ["login"]


def browse_manifest(session: dict[str, str | bool]) -> int:
    user = str(session["user"])
    paths = get_user_paths(user or None)
    items = load_manifest(paths["manifest"])
    if not items:
        print("\nNo manifest entries were found.")
        print_help_text("Run 'Update manifest' first, then come back here to browse games.")
        return 0

    items = sorted(items, key=lambda item: str(getattr(item, "title", "")).lower())
    filtered_items = items
    page_size = 20
    page = 0

    while True:
        total_pages = max(1, (len(filtered_items) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        start = page * page_size
        end = min(start + page_size, len(filtered_items))

        print("")
        print(f"Manifest browser for profile: {user or 'default'}")
        print(f"Showing {start + 1}-{end} of {len(filtered_items)} game(s)")
        print("Use the slug/title or numeric id from this list in the game filter prompts.")
        print("")

        for index, item in enumerate(filtered_items[start:end], start=start + 1):
            slug = getattr(item, "title", "")
            long_title = getattr(item, "long_title", "")
            game_id = getattr(item, "id", "")
            if long_title and long_title != slug:
                print(f"{index:4}. {long_title}")
                print(f"      slug: {slug} | id: {game_id}")
            else:
                print(f"{index:4}. {slug} | id: {game_id}")

        print("")
        print("[Enter] next page | p previous | s search | a show all | q return to menu")
        choice = input("Browse command: ").strip().lower()

        if not choice:
            page = 0 if page >= total_pages - 1 else page + 1
            continue
        if choice == "q":
            return 0
        if choice == "p":
            page = max(0, page - 1)
            continue
        if choice == "a":
            filtered_items = items
            page = 0
            continue
        if choice == "s":
            query = prompt_text(
                "Search text",
                "",
                "Enter part of a title, slug, or numeric id to filter the manifest list. Leave blank to clear the filter.",
            ).lower()
            if not query:
                filtered_items = items
            else:
                filtered_items = [
                    item for item in items
                    if query in str(getattr(item, "title", "")).lower()
                    or query in str(getattr(item, "long_title", "")).lower()
                    or query in str(getattr(item, "id", "")).lower()
                ]
            if not filtered_items:
                print_help_text("No games matched that search. Returning to the full manifest list.")
                filtered_items = items
            page = 0
            continue

        print_help_text("Use Enter, p, s, a, or q.")


def build_update_args(session: dict[str, str | bool]) -> list[str]:
    args = ["update"]
    mode = prompt_choice(
        "Update mode",
        ["standard", "skipknown", "updateonly", "full"],
        "standard",
        {
            "standard": "Normal update. Refresh games already known in the manifest and pick up changes.",
            "skipknown": "Only add games that are not already in your manifest.",
            "updateonly": "Only process games that GOG reports as updated.",
            "full": "Fetch your full library regardless of what is already in the manifest.",
        },
    )
    if mode != "standard":
        args.append(f"-{mode}")

    prompt_filters(args)

    installers = prompt_choice(
        "Installer type",
        ["standalone", "both"],
        "standalone",
        {
            "standalone": "Offline installers only.",
            "both": "Include both offline installers and Galaxy installers.",
        },
    )
    args.extend(["-installers", installers])

    resumemode = "resume"
    if prompt_show_advanced("for update"):
        resumemode = prompt_choice(
            "Resume mode",
            ["resume", "noresume", "onlyresume"],
            "resume",
            {
                "resume": "Continue an interrupted update if resume data exists.",
                "noresume": "Ignore any saved resume state and start fresh.",
                "onlyresume": "Resume only; do not continue into a fresh update afterward.",
            },
        )
        if prompt_bool("Skip hidden games", False, "Ignore games marked hidden in your GOG library on the GOG side."):
            args.append("-skiphidden")
        if prompt_bool("Strict verify", False, "Use stricter file change detection during manifest updates."):
            args.append("-strictverify")
        if prompt_bool("Strict duplicate handling", False, "Be more aggressive about removing duplicate download entries."):
            args.append("-strictdupe")
        if prompt_bool("Download MD5 XML files", False, "Fetch extra MD5 metadata files when GOG provides them."):
            args.append("-md5xmls")
        if prompt_bool("Skip changelog saving", False, "Do not store changelog text in the manifest."):
            args.append("-nochangelogs")

        wait_hours = prompt_text(
            "Wait before starting in hours",
            "",
            "Enter the number of hours to wait before starting, such as 2 or 0.5. Leave blank to start immediately.",
        )
        if wait_hours:
            args.extend(["-wait", wait_hours])
    args.extend(["-resumemode", resumemode])

    return args


def build_download_args(session: dict[str, str | bool]) -> list[str]:
    args = ["download"]
    savedir = prompt_text(
        "Download directory",
        "",
        "Optional. Choose where downloads should be stored. Leave blank to use the default games directory.",
    )
    if savedir:
        args.append(savedir)

    prompt_filters(args)

    if prompt_bool("Skip extras", False, "Do not download bonus items such as manuals, wallpapers, or extras."):
        args.append("-skipextras")
    if prompt_bool("Skip Galaxy installers", False, "Do not download Galaxy-specific installers."):
        args.append("-skipgalaxy")
    if prompt_bool("Skip standalone installers", False, "Do not download standalone offline installers."):
        args.append("-skipstandalone")
    if prompt_bool("Skip shared installers", False, "Do not download files shared by both Galaxy and standalone installers."):
        args.append("-skipshared")
    if prompt_show_advanced("for download"):
        if prompt_bool("Download cover images", False, "Save cover art into each game's !images folder."):
            args.append("-covers")
        if prompt_bool("Download background images", False, "Save background artwork into each game's !images folder."):
            args.append("-backgrounds")
        if prompt_bool("Skip file preallocation", False, "Do not reserve the full file size before downloading. This may reduce compatibility but can increase fragmentation."):
            args.append("-skippreallocation")
        if prompt_bool("Keep old images instead of cleaning them", False, "Leave older downloaded images in place instead of cleaning them up."):
            args.append("-nocleanimages")

        add_multi_value_args(
            args,
            "-skipfiles",
            prompt_list(
                "Filename patterns to skip",
                "",
                "Enter filenames or wildcard patterns to exclude, such as *.pdf or setup_old_*.exe. Leave blank to skip nothing.",
            ),
        )

        wait_hours = prompt_text(
            "Wait before starting in hours",
            "",
            "Enter the number of hours to wait before starting, such as 2 or 0.5. Leave blank to start immediately.",
        )
        if wait_hours:
            args.extend(["-wait", wait_hours])

        download_limit = prompt_text(
            "Download limit in MB",
            "",
            "Optional. Enter a maximum download size in megabytes for this run. Leave blank for no limit.",
        )
        if download_limit:
            args.extend(["-downloadlimit", download_limit])

    return args


def build_verify_args(session: dict[str, str | bool]) -> list[str]:
    args = ["verify"]
    gamedir = prompt_text(
        "Games directory",
        "",
        "Optional. Choose the directory containing your downloaded games. Leave blank to use the default.",
    )
    if gamedir:
        args.append(gamedir)

    prompt_filters(args)

    if prompt_bool("Delete failed files", False, "Delete files that fail verification checks."):
        args.append("-delete")
    if prompt_bool("Do not clean failed files", False, "Leave failed files in place instead of cleaning them up."):
        args.append("-noclean")
    if prompt_bool("Skip extras", False, "Do not verify bonus items such as manuals, wallpapers, or extras."):
        args.append("-skipextras")
    if prompt_bool("Skip Galaxy installers", False, "Do not verify Galaxy-specific installers."):
        args.append("-skipgalaxy")
    if prompt_bool("Skip standalone installers", False, "Do not verify standalone offline installers."):
        args.append("-skipstandalone")
    if prompt_bool("Skip shared installers", False, "Do not verify files shared by both Galaxy and standalone installers."):
        args.append("-skipshared")
    if prompt_show_advanced("for verify"):
        if prompt_bool("Skip MD5 checks", False, "Do not verify file hashes. Faster, but less thorough."):
            args.append("-skipmd5")
        if prompt_bool("Skip size checks", False, "Do not compare file sizes to the manifest."):
            args.append("-skipsize")
        if prompt_bool("Skip zip checks", False, "Do not test zip archive integrity."):
            args.append("-skipzip")
        if prompt_bool("Force verification of unchanged files", False, "Verify files even if they were previously marked as unchanged."):
            args.append("-forceverify")
        if prompt_bool("Clear change flags for files that pass", False, "Clear change markers when files verify successfully."):
            args.append("-permissivechangeclear")

        add_multi_value_args(
            args,
            "-skipfiles",
            prompt_list(
                "Filename patterns to skip",
                "",
                "Enter filenames or wildcard patterns to exclude from verification. Leave blank to skip nothing.",
            ),
        )
    return args


def build_clean_args(session: dict[str, str | bool]) -> list[str]:
    args = ["clean", prompt_text("Directory to clean", "games", "Directory that should be scanned for orphaned files.")]
    return args


def build_clear_partial_args(session: dict[str, str | bool]) -> list[str]:
    args = ["clear_partial_downloads", prompt_text("Games directory", "games", "Directory containing partial downloads to remove.")]
    return args


def build_trash_args(session: dict[str, str | bool]) -> list[str]:
    args = ["trash", prompt_text("Games directory", "games", "Directory containing orphaned files to permanently remove.")]
    if prompt_show_advanced("for trash"):
        if prompt_bool("Delete installer files", False, "Remove orphaned installer files when trashing."):
            args.append("-installers")
        if prompt_bool("Delete image folders", False, "Remove orphaned !images folders when trashing."):
            args.append("-images")
    return args


def build_import_args(session: dict[str, str | bool]) -> list[str]:
    args = [
        "import",
        prompt_required_text("Source directory", "Directory to scan for existing files you want to import."),
        prompt_text("Destination directory", "games", "Managed games directory where matched files should be placed."),
    ]
    prompt_filters(args)

    if prompt_show_advanced("for import"):
        if prompt_bool("Skip Galaxy installers", False):
            args.append("-skipgalaxy")
        if prompt_bool("Skip standalone installers", False):
            args.append("-skipstandalone")
        if prompt_bool("Skip shared installers", False):
            args.append("-skipshared")
    return args


def build_backup_args(session: dict[str, str | bool]) -> list[str]:
    args = [
        "backup",
        prompt_text("Source directory", "games", "Managed games directory to back up from."),
        prompt_required_text("Backup destination directory", "Directory where backup copies should be written."),
    ]
    prompt_filters(args)

    if prompt_bool("Skip extras", False):
        args.append("-skipextras")
    if prompt_show_advanced("for backup"):
        if prompt_bool("Skip Galaxy installers", False):
            args.append("-skipgalaxy")
        if prompt_bool("Skip standalone installers", False):
            args.append("-skipstandalone")
        if prompt_bool("Skip shared installers", False):
            args.append("-skipshared")
    return args


def prompt_extra_switches() -> list[str]:
    if not prompt_show_advanced("to append raw CLI flags"):
        return []
    raw = prompt_text(
        "Advanced command-line options to append",
        "",
        "Optional escape hatch for extra CLI flags not covered by the menu. Example: --user alice or additional command-specific flags.",
    )
    return shlex.split(raw) if raw else []


def format_command(command: list[str]) -> str:
    return subprocess.list2cmdline(command)


def ensure_dryrun_flag(command_args: list[str]) -> list[str]:
    if "-dryrun" in command_args:
        return command_args[:]
    return command_args + ["-dryrun"]


def remove_dryrun_flag(command_args: list[str]) -> list[str]:
    return [arg for arg in command_args if arg != "-dryrun"]


def run_command(command_args: list[str], confirm_label: str = "Run this command") -> int:
    command = [sys.executable, str(SCRIPT_PATH)] + command_args
    print("\nCommand:")
    print(f"  {format_command(command)}")

    if not prompt_bool(confirm_label, True):
        print("Cancelled.")
        return 0

    print("")
    return subprocess.call(command)


def format_session_value(session: dict[str, str | bool], key: str) -> str:
    value = session[key]
    if key == "user":
        return str(value) or "default"
    return "off" if bool(value) and key == "nolog" else "on" if key == "nolog" else "on" if bool(value) else "off"


def print_menu_help(actions: dict[str, tuple[str, object, str]], selected_key: str | None = None) -> None:
    print("")
    if selected_key:
        label, _, description = actions[selected_key]
        print(f"{selected_key}. {label}")
        print_help_text(description)
        return

    print("Menu Help")
    print("---------")
    print_help_text("Type a menu key to choose an action.")
    print_help_text("Type ? to show help for all menu items.")
    print_help_text("Type ? followed by a menu key, such as ? 9, to explain one item.")
    print("")
    for key, (label, _, description) in actions.items():
        print(f"{key}. {label}")
        print_help_text(description)
        print("")


def choose_action(session: dict[str, str | bool]) -> str:
    actions = {
        "0": ("Session settings", None, "Change session-wide settings such as the user profile, debug output, and logfile behavior."),
        "1": ("Login", build_login_args, "Open the GOG login flow in your browser and save a local token for later commands."),
        "2": ("Update manifest", build_update_args, "Fetch your current GOG library and file metadata into the local manifest used by download and verify."),
        "3": ("Browse games in manifest", None, "View the games already stored in your local manifest so you can find titles, slugs, and numeric IDs for filtering."),
        "4": ("Download files", build_download_args, "Download games and extras described by the local manifest into your managed games directory."),
        "5": ("Verify files", build_verify_args, "Check downloaded files against the manifest and optionally clean up or delete files that fail verification."),
        "6": ("Clean orphaned files", build_clean_args, "Move files that are no longer tracked by the manifest out of the main games tree into the orphan area."),
        "7": ("Clear partial downloads", build_clear_partial_args, "Remove leftover partial download files from interrupted downloads."),
        "8": ("Trash orphaned files", build_trash_args, "Permanently delete orphaned files that were previously moved out of the main managed collection."),
        "9": ("Import existing files", build_import_args, "Scan another directory for files that match the manifest and copy them into the managed collection instead of re-downloading them."),
        "10": ("Backup files", build_backup_args, "Copy files from one managed collection to another location as an incremental backup."),
        "11": ("Quit", None, "Exit the menu without running another command."),
    }

    print("\nGOGRepoC Menu")
    print("--------------")
    print(f"Profile: {format_session_value(session, 'user')}")
    print(f"Debug: {format_session_value(session, 'debug')}")
    print(f"Log file: {format_session_value(session, 'nolog')}")
    print("")
    print("Type ? for menu help.\n")
    for key, (label, _, _) in actions.items():
        print(f"{key}. {label}")

    while True:
        choice = input("\nSelect an action: ").strip()
        normalized = choice.lower()
        if normalized in {"?", "h", "help"}:
            print_menu_help(actions)
            continue
        if normalized.startswith("?"):
            requested = normalized[1:].strip()
            if requested in actions:
                print_menu_help(actions, requested)
            else:
                print_help_text("Use ? by itself for all menu help, or ? followed by a menu key such as ? 9.")
            continue
        if normalized in actions:
            return normalized
        print("Enter one of the listed menu keys.")


def main() -> int:
    print("Interactive wrapper for gogrepoc.py")
    print("Press Ctrl+C at any prompt to exit.\n")
    print("Type ? at many prompts to see what an option means.\n")

    session = prompt_session_settings()

    actions = {
        "1": (build_login_args, False),
        "2": (build_update_args, False),
        "3": (browse_manifest, None),
        "4": (build_download_args, True),
        "5": (build_verify_args, False),
        "6": (build_clean_args, True),
        "7": (build_clear_partial_args, True),
        "8": (build_trash_args, True),
        "9": (build_import_args, False),
        "10": (build_backup_args, False),
    }

    while True:
        choice = choose_action(session)
        match choice:
            case "11":
                return 0
            case "0":
                session = prompt_session_settings(session)
                print("")
                continue
            case "3":
                build_args, _ = actions[choice]
                exit_code = build_args(session)
                print(f"\nExit code: {exit_code}")
                if not prompt_bool("Return to menu", True):
                    return exit_code
                print("")
                continue
            case _:
                build_args, supports_dryrun = actions[choice]
                command_args = session_args(session) + build_args(session)
                command_args.extend(prompt_extra_switches())

                execution_mode = prompt_execution_mode(supports_dryrun)
                if execution_mode == "preview":
                    exit_code = run_command(ensure_dryrun_flag(command_args), "Run preview command")
                elif execution_mode == "preview_then_run":
                    exit_code = run_command(ensure_dryrun_flag(command_args), "Run preview command")
                    if exit_code == 0:
                        print("")
                        exit_code = run_command(remove_dryrun_flag(command_args), "Run real command after preview")
                else:
                    exit_code = run_command(remove_dryrun_flag(command_args))

        print(f"\nExit code: {exit_code}")

        if not prompt_bool("Return to menu", True):
            return exit_code
        print("")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nExiting.")
        raise SystemExit(1)

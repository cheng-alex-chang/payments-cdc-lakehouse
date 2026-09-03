"""Static structural validation of the Databricks Asset Bundle.

This runs with no credentials and no network, which is what makes it safe on fork pull
requests where secrets are unavailable. It deliberately does NOT replace
`databricks bundle validate`: that command authenticates (the dev target is
`mode: development`, which resolves workspace identity to prefix resources), so it only
runs on trusted triggers. See .github/workflows/ci.yml.

What it catches that a YAML linter would not: a renamed notebook that a resource still
points at, an `include:` glob that no longer matches anything, and a
`${resources.<kind>.<key>...}` reference to a resource that does not exist.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

BUNDLE_ROOT = Path(__file__).resolve().parents[1] / "databricks"

# ${resources.pipelines.payments_dlt.id} -> ("pipelines", "payments_dlt")
_REFERENCE = re.compile(r"\$\{resources\.([a-z_]+)\.([A-Za-z0-9_]+)\.[A-Za-z0-9_.]+\}")

# Keys whose value is a path to a file that must exist in the checkout.
_PATH_KEYS = ("notebook_path", "path")


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise SystemExit(f"{path}: expected a YAML mapping, got {type(loaded).__name__}")
    return loaded


def resource_files(bundle: dict, root: Path) -> list[Path]:
    """Every file matched by the bundle's `include:` globs.

    An include that matches nothing is an error rather than a no-op: it means a resource
    file was renamed or deleted and the bundle silently stopped deploying it.
    """
    files: list[Path] = []
    for pattern in bundle.get("include", []):
        matched = sorted(root.glob(pattern))
        if not matched:
            raise SystemExit(f"include pattern matched no files: {pattern}")
        files.extend(matched)
    return files


def walk(node: object, path_keys: tuple[str, ...] = _PATH_KEYS):
    """Yield (key, value) for every scalar mapping entry, at any depth."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str):
                yield key, value
            else:
                yield from walk(value, path_keys)
    elif isinstance(node, list):
        for item in node:
            yield from walk(item, path_keys)


def check_paths(resource_file: Path, document: dict) -> list[str]:
    """Notebook/library paths are relative to the file that declares them."""
    errors = []
    for key, value in walk(document):
        if key not in _PATH_KEYS or value.startswith("${"):
            continue
        target = (resource_file.parent / value).resolve()
        if not target.is_file():
            errors.append(f"{resource_file.name}: {key} -> {value} does not exist")
    return errors


def check_references(documents: dict[Path, dict]) -> list[str]:
    """Every ${resources.<kind>.<key>...} must name a resource the bundle declares."""
    declared: set[tuple[str, str]] = set()
    for document in documents.values():
        for kind, entries in (document.get("resources") or {}).items():
            if isinstance(entries, dict):
                declared.update((kind, name) for name in entries)

    errors = []
    for resource_file, document in documents.items():
        for _, value in walk(document):
            for kind, name in _REFERENCE.findall(value):
                if (kind, name) not in declared:
                    errors.append(
                        f"{resource_file.name}: reference to undeclared resource "
                        f"{kind}.{name}"
                    )
    return errors


def main(root: Path = BUNDLE_ROOT) -> None:
    bundle_file = root / "databricks.yml"
    if not bundle_file.is_file():
        raise SystemExit(f"bundle file not found: {bundle_file}")

    bundle = load_yaml(bundle_file)

    if not (bundle.get("bundle") or {}).get("name"):
        raise SystemExit("databricks.yml: bundle.name is missing or empty")

    targets = bundle.get("targets") or {}
    if not targets:
        raise SystemExit("databricks.yml: no targets declared")

    defaults = [name for name, spec in targets.items() if (spec or {}).get("default")]
    if len(defaults) > 1:
        raise SystemExit(f"databricks.yml: more than one default target: {sorted(defaults)}")

    documents = {path: load_yaml(path) for path in resource_files(bundle, root)}

    errors: list[str] = []
    for path, document in documents.items():
        if not isinstance(document.get("resources"), dict):
            errors.append(f"{path.name}: missing a top-level `resources:` mapping")
        errors.extend(check_paths(path, document))
    errors.extend(check_references(documents))

    if errors:
        for error in errors:
            LOGGER.error("%s", error)
        raise SystemExit(f"Bundle validation failed with {len(errors)} error(s)")

    LOGGER.info(
        "Bundle '%s' is structurally valid: %d target(s), %d resource file(s)",
        bundle["bundle"]["name"],
        len(targets),
        len(documents),
    )
    print("Bundle structurally valid")


if __name__ == "__main__":  # pragma: no cover
    main()

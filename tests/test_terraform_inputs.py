"""Terraform variables whose defaults would be destructive if the value went missing.

Most variables benefit from a sensible default. These two do not, and the difference is
what an *unset* value makes Terraform plan:

* `s3_bucket` defaulted to "payments-lake-changeme". Unset, that is not a placeholder --
  Terraform plans to repoint the storage integration and the external stage at a bucket
  that does not exist, and COPY INTO starts failing against a live warehouse.
* `etl_role_users` defaulted to []. An empty list is a legitimate intent ("grant to
  nobody") and therefore indistinguishable from a forgotten variable -- so Terraform plans
  to DESTROY the grant the pipeline authenticates with.

The release workflow hit both at once: `0 to add, 3 to change, 1 to destroy` against
production. It failed only because an unrelated IAM permission was missing, which is luck
rather than a safeguard. Both must now fail closed, so a missing value stops the run.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SNOWFLAKE_TF = Path(__file__).resolve().parents[1] / "infra" / "terraform" / "snowflake" / "variables.tf"

MUST_FAIL_CLOSED = ("s3_bucket", "etl_role_users")


def _variable_block(name: str, source: str) -> str:
    match = re.search(rf'variable\s+"{re.escape(name)}"\s*\{{', source)
    assert match, f"variable {name!r} not found in {SNOWFLAKE_TF.name}"
    depth, start = 0, match.end() - 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"unbalanced braces around variable {name!r}")


@pytest.mark.parametrize("name", MUST_FAIL_CLOSED)
def test_destructive_variables_have_no_default(name: str) -> None:
    block = _variable_block(name, SNOWFLAKE_TF.read_text(encoding="utf-8"))
    # `default` at the top level of the block -- not the word appearing in a comment.
    defaults = [
        line for line in block.splitlines()
        if re.match(r"\s*default\s*=", line) and not line.lstrip().startswith("#")
    ]
    assert not defaults, (
        f"variable {name!r} declares a default ({defaults}). Unset, that default is applied "
        "to production rather than stopping the run -- see this module's docstring."
    )

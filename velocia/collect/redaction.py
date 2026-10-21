"""Secret redaction (docs/06-design.md §8, docs/05-quality-and-release.md §4).

Every writer of the results directory, reports and bundles passes Spark configs, environment
variables and event-log property maps through a :class:`Redactor` first.

A key is secret when it matches the built-in deny-list or a user pattern (shell-style globs,
case-insensitive). Under a secret key the whole value is replaced, whatever its type. In free
text, ``KEY=value`` assignments with a secret key and the password in ``scheme://user:pw@host``
URLs are replaced, which covers environment lines and ``--conf`` options in command lines.

The deny-list is deliberately broad (``*key*`` also matches harmless keys such as
``spark.io.encryption.keySizeBits``): losing a harmless value is acceptable, leaking a secret is
not. For the same reason, apply :meth:`Redactor.redact` to property maps and free text, not to
Velocia's own records, whose field names (``cell_key``) would match too.
"""

import fnmatch
import re
from collections.abc import Iterable, Mapping
from typing import Any

REDACTED = "*********(redacted)"  # same placeholder Apache Spark uses

DENY_LIST = (
    "*password*",
    "*passwd*",
    "*secret*",
    "*token*",
    "*key*",
    "*credential*",
)

_ASSIGNMENT = re.compile(r"(?<![\w.\-])([\w.\-]+)=")
# The password runs to the last "@" of the URL: AWS-style secrets contain "/" and some contain "@".
_URL_PASSWORD = re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*://[^/\s:@]*:)(\S+)(@)")
_WHITESPACE = re.compile(r"\s")


class Redactor:
    def __init__(self, extra_patterns: Iterable[str] = ()) -> None:
        patterns = (*DENY_LIST, *extra_patterns)
        self._secret_key = re.compile(
            "|".join(f"(?:{fnmatch.translate(p)})" for p in patterns), re.IGNORECASE
        )

    def is_secret_key(self, key: str) -> bool:
        return self._secret_key.fullmatch(key) is not None

    def redact(self, value: Any) -> Any:
        """Return a redacted copy of a mapping, list, string or scalar. Never mutates ``value``."""
        if isinstance(value, str):
            return self.redact_text(value)
        if isinstance(value, Mapping):
            return {
                self.redact(k): REDACTED if self._is_secret(k) else self.redact(v)
                for k, v in value.items()
            }
        if isinstance(value, list | tuple):
            # A [key, value] pair, as in older event logs and Spark's getAll().
            if len(value) == 2 and self._is_secret(value[0]):
                items = [self.redact(value[0]), REDACTED]
            else:
                items = [self.redact(v) for v in value]
            return tuple(items) if isinstance(value, tuple) else items
        return value

    def _is_secret(self, key: object) -> bool:
        return isinstance(key, str) and self.is_secret_key(key)

    def redact_text(self, text: str) -> str:
        text = _URL_PASSWORD.sub(rf"\g<1>{REDACTED}\g<3>", text)
        parts: list[str] = []
        pos = 0
        for match in _ASSIGNMENT.finditer(text):
            if match.start() < pos or not self.is_secret_key(match.group(1)):
                continue
            parts += [text[pos : match.end()], REDACTED]
            pos = _value_end(text, match.end())
        parts.append(text[pos:])
        return "".join(parts)


def _value_end(text: str, start: int) -> int:
    """End of the assigned value: a quoted string, or everything up to the next whitespace."""
    if start < len(text) and text[start] in "\"'":
        close = text.find(text[start], start + 1)
        return len(text) if close == -1 else close + 1
    match = _WHITESPACE.search(text, start)
    return len(text) if match is None else match.start()


def redact(value: Any, extra_patterns: Iterable[str] = ()) -> Any:
    return Redactor(extra_patterns).redact(value)

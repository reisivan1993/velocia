import copy
import json
import string

import pytest
from conftest import SECRET_MARKER, PlantedSecrets
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from velocia.collect.redaction import REDACTED, Redactor, redact
from velocia.config.models import VelociaConfig

DEFAULT = Redactor()

# --- strategies -------------------------------------------------------------------------------

SECRET_WORDS = ["password", "passwd", "secret", "token", "key", "credential", "accessKey"]
KEY_CHARS = string.ascii_letters + string.digits + "._-"


@st.composite
def secret_keys(draw: st.DrawFn) -> str:
    word = draw(st.sampled_from(SECRET_WORDS))
    flips = draw(st.lists(st.booleans(), min_size=len(word), max_size=len(word)))
    word = "".join(c.upper() if f else c for c, f in zip(word, flips, strict=True))
    prefix = draw(st.text(KEY_CHARS, max_size=12))
    suffix = draw(st.text(KEY_CHARS, max_size=12))
    return prefix + word + suffix


safe_keys = st.from_regex(r"[a-z][a-z0-9._]{0,20}", fullmatch=True).filter(
    lambda k: not DEFAULT.is_secret_key(k)
)
# Free text without assignments or URLs: nothing in it may be redacted.
safe_text = st.text(string.ascii_letters + string.digits + " .,:/-_()", max_size=30).filter(
    lambda t: SECRET_MARKER not in t and "://" not in t and not DEFAULT.is_secret_key(t)
)
safe_scalars = st.one_of(st.none(), st.booleans(), st.integers(), safe_text)
# Secrets in free text end at whitespace and don't start with a quote (see redact_text).
secret_values = st.builds(
    lambda a, b: a + SECRET_MARKER + b,
    st.text(string.ascii_letters + string.digits + "/+=@:-_.,;!#$%", max_size=10),
    st.text(string.ascii_letters + string.digits + "/+=@:-_.,;!#$%\"'", max_size=10),
)
any_secret_value = st.one_of(
    secret_values,
    secret_values.map(lambda v: v + " with spaces"),
    secret_values.map(lambda v: [v]),
    secret_values.map(lambda v: {"nested": v}),
)
planted_leaves = st.one_of(
    st.dictionaries(secret_keys(), any_secret_value, min_size=1, max_size=3),
    st.builds(lambda k, v: [k, v], secret_keys(), any_secret_value),
    st.builds(
        lambda pre, k, v, post: f"{pre} {k}={v} {post}",
        safe_text,
        secret_keys(),
        secret_values,
        safe_text,
    ),
    st.builds(lambda k, v: f"--conf '{k}={v.replace(chr(39), '')}'", secret_keys(), secret_values),
    st.builds(
        lambda u, v: f"s3a://{u}:{v}@bucket/path",
        st.text(string.ascii_letters, max_size=8),
        secret_values,
    ),
)


def trees(leaves: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
    return st.recursive(
        leaves,
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(safe_keys, children, max_size=4),
        ),
        max_leaves=12,
    )


# --- properties -------------------------------------------------------------------------------


@given(trees(st.one_of(safe_scalars, planted_leaves)))
def test_planted_secrets_never_survive(tree: object) -> None:
    assert SECRET_MARKER not in repr(redact(tree))


@given(trees(safe_scalars))
def test_values_without_secrets_are_unchanged(tree: object) -> None:
    assert redact(tree) == tree


@given(trees(st.one_of(safe_scalars, planted_leaves)))
def test_redaction_is_idempotent_and_does_not_mutate(tree: object) -> None:
    before = copy.deepcopy(tree)
    once = redact(tree)
    assert tree == before
    assert redact(once) == once


# --- planted-secret fixtures ------------------------------------------------------------------


def test_fixture_confs_env_and_event_log_are_redacted(planted_secrets: PlantedSecrets) -> None:
    redactor = Redactor(planted_secrets.user_patterns)
    conf = redactor.redact(planted_secrets.spark_conf)
    env = redactor.redact(planted_secrets.env)
    event = redactor.redact(json.loads(planted_secrets.event_log_line))
    for written in (conf, env, event):
        planted_secrets.assert_absent(json.dumps(written))

    # What isn't secret stays readable.
    assert conf["spark.app.name"] == "tpch-q1"
    assert conf["spark.velocia.nested"]["db"]["user"] == "bench"
    assert conf["spark.driver.extraJavaOptions"].startswith("-Xss4m ")
    assert conf["spark.eventLog.dir"] == f"s3a://AKIAXXXX:{REDACTED}@bucket/logs"
    assert env["HOME"] == "/home/spark"
    command = event["System Properties"]["sun.java.command"]
    assert command.startswith("org.apache.spark.deploy.SparkSubmit --conf ")
    assert command.endswith(" velocia_driver.py")
    assert event["Classpath Entries"] == {
        "/opt/spark/jars/spark-core_2.12-3.5.9.jar": "System Classpath"
    }


def test_user_patterns_extend_the_deny_list(planted_secrets: PlantedSecrets) -> None:
    assert SECRET_MARKER in redact(planted_secrets.env)["MYCORP_VAULT_ADDR"]
    assert redact(planted_secrets.env, ["*VAULT*"])["MYCORP_VAULT_ADDR"] == REDACTED


def test_assert_absent_checks_files_and_trees(tmp_path, planted_secrets: PlantedSecrets) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "run").mkdir()
    (tmp_path / "run" / "manifest.json").write_text(json.dumps(planted_secrets.spark_conf))
    with pytest.raises(AssertionError, match=r"manifest\.json"):
        planted_secrets.assert_absent(tmp_path)


# --- details ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("user=bench;password=hunter2;db=tpch", f"user=bench;password={REDACTED}"),
        ('TOKEN="a b c" next', f"TOKEN={REDACTED} next"),
        ("TOKEN='unterminated value", f"TOKEN={REDACTED}"),
        ("export AWS_SECRET_ACCESS_KEY=abc", f"export AWS_SECRET_ACCESS_KEY={REDACTED}"),
        ("-Dspark.ssl.keyPassword=x -Dfoo=bar", f"-Dspark.ssl.keyPassword={REDACTED} -Dfoo=bar"),
        ("jdbc:postgresql://bench:pw@db:5432/x", f"jdbc:postgresql://bench:{REDACTED}@db:5432/x"),
        ("spark://master:7077", "spark://master:7077"),
        ("a == b, x=1", "a == b, x=1"),
    ],
)
def test_redact_text(text: str, expected: str) -> None:
    assert DEFAULT.redact_text(text) == expected


def test_pairs_keep_their_container_type() -> None:
    assert redact([" key=s ", None]) == [f" key={REDACTED} ", REDACTED]
    assert redact(("spark.authenticate.secret", "s")) == ("spark.authenticate.secret", REDACTED)
    assert redact([["spark.app.name", "q1"]]) == [["spark.app.name", "q1"]]


def test_mapping_keys_are_redacted_as_text() -> None:
    assert redact({"TOKEN=s": 1}) == {f"TOKEN={REDACTED}": REDACTED}


def test_config_accepts_key_patterns() -> None:
    config = VelociaConfig.model_validate({"redaction": {"key_patterns": ["*vault*"]}})
    assert config.redaction.key_patterns == ["*vault*"]
    assert VelociaConfig().redaction.key_patterns == []
    with pytest.raises(ValidationError):
        VelociaConfig.model_validate({"redaction": {"key_patterns": [""]}})

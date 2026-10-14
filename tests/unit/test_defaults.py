"""The UI defaults, sourcetypes and props.conf stay consistent with each other."""

import configparser
import json
import os

import nats_kv_command
import nats_subscribe_command

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


def load_global_config():
    with open(os.path.join(ROOT, "globalConfig.json")) as f:
        return json.load(f)


def load_props():
    props = configparser.ConfigParser(interpolation=None, strict=False)
    props.optionxform = str
    props.read(os.path.join(ROOT, "package", "default", "props.conf"))
    return props


def service(name):
    services = load_global_config()["pages"]["inputs"]["services"]
    return next(s for s in services if s["name"] == name)


def entity(service_name, field):
    return next(e for e in service(service_name)["entity"] if e["field"] == field)


def test_subscribe_subject_has_no_default_and_explains_wildcards():
    subject = entity("nats_subscribe", "subject")

    assert "defaultValue" not in subject
    assert subject["required"] is True
    assert "* matches exactly one token" in subject["help"]
    assert "> matches all remaining tokens" in subject["help"]


def test_input_and_command_sourcetypes_match():
    assert (
        entity("nats_subscribe", "sourcetype")["defaultValue"]
        == nats_subscribe_command.SOURCETYPE
    )
    assert entity("nats_kv", "sourcetype")["defaultValue"] == nats_kv_command.SOURCETYPE


def test_every_sourcetype_has_props():
    props = load_props()

    for sourcetype in ("nats:message", "nats:json", "nats:kv"):
        stanza = props[sourcetype]
        assert stanza["SHOULD_LINE_MERGE"] == "false"
        assert int(stanza["TRUNCATE"]) >= 1048576
        # The inputs set _time; never guess one from the payload.
        assert stanza["DATETIME_CONFIG"] == "NONE"

    assert props["nats:json"]["KV_MODE"] == "json"
    assert props["nats:kv"]["KV_MODE"] == "json"


def test_index_defaults_to_an_existing_index():
    # UCC's implicit "default" is not an index name, so the combobox showed
    # "default (New value)".
    for name in ("nats_subscribe", "nats_kv"):
        assert entity(name, "index")["defaultValue"] == "main"


def test_ui_titles_and_input_type_column():
    config = load_global_config()
    account_tab = config["pages"]["configuration"]["tabs"][0]
    header = config["pages"]["inputs"]["table"]["header"]

    assert account_tab["formTitle"] == "Account"
    assert {"label": "Input Type", "field": "serviceTitle"} in header


def test_natssubscribe_has_count_and_duration_options():
    command = next(
        c
        for c in load_global_config()["customSearchCommand"]
        if c["commandName"] == "natssubscribe"
    )
    arguments = {a["name"]: a for a in command["arguments"]}

    assert arguments["count"]["validate"] == {"type": "Integer", "minimum": 0}
    assert arguments["duration"]["validate"]["minimum"] == 1
    assert "count=" in command["syntax"] and "duration=" in command["syntax"]

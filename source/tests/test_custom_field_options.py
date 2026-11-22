"""Tests for custom field context and option streams."""

from __future__ import annotations

import typing as t

import pytest
import requests

from tap_jira.streams import (
    CustomFieldContextProjectMappingStream,
    CustomFieldContextStream,
    CustomFieldOptionStream,
    FieldStream,
)
from tap_jira.tap import TapJira

if t.TYPE_CHECKING:
    from collections.abc import Iterable


@pytest.fixture
def tap() -> TapJira:
    """Create a tap instance with a minimal config."""
    return TapJira(
        config={
            "domain": "test.atlassian.net",
            "email": "test@example.com",
            "api_token": "test-token",
        },
    )


class TestFieldStreamChildContext:
    """Tests for FieldStream.get_child_context."""

    def test_system_field_yields_no_context(self, tap: TapJira) -> None:
        stream = FieldStream(tap)
        record = {"id": "status", "custom": False}
        assert stream.get_child_context(record, None) is None

    def test_field_without_custom_flag_yields_no_context(
        self,
        tap: TapJira,
    ) -> None:
        stream = FieldStream(tap)
        record = {"id": "customfield_10000"}
        assert stream.get_child_context(record, None) is None

    def test_field_without_id_yields_no_context(self, tap: TapJira) -> None:
        stream = FieldStream(tap)
        record = {"custom": True, "schema": {"type": "option"}}
        assert stream.get_child_context(record, None) is None

    def test_option_field_supports_options(self, tap: TapJira) -> None:
        stream = FieldStream(tap)
        record = {
            "id": "customfield_10000",
            "custom": True,
            "schema": {"type": "option"},
        }
        assert stream.get_child_context(record, None) == {
            "fieldId": "customfield_10000",
            "supports_options": True,
        }

    def test_option_with_child_items_field_supports_options(
        self,
        tap: TapJira,
    ) -> None:
        stream = FieldStream(tap)
        record = {
            "id": "customfield_10001",
            "custom": True,
            "schema": {"type": "array", "items": "option-with-child"},
        }
        assert stream.get_child_context(record, None) == {
            "fieldId": "customfield_10001",
            "supports_options": True,
        }

    def test_non_option_field_does_not_support_options(self, tap: TapJira) -> None:
        stream = FieldStream(tap)
        record = {
            "id": "customfield_10002",
            "custom": True,
            "schema": {"type": "string"},
        }
        assert stream.get_child_context(record, None) == {
            "fieldId": "customfield_10002",
            "supports_options": False,
        }

    def test_field_without_schema_does_not_support_options(
        self,
        tap: TapJira,
    ) -> None:
        stream = FieldStream(tap)
        record = {"id": "customfield_10003", "custom": True}
        assert stream.get_child_context(record, None) == {
            "fieldId": "customfield_10003",
            "supports_options": False,
        }


class TestCustomFieldContextStream:
    """Tests for CustomFieldContextStream."""

    def test_primary_keys_are_composite(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        assert stream.primary_keys == ("fieldId", "id")

    def test_schema_includes_project_ids(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        prop = stream.schema["properties"]["projectIds"]
        assert prop == {"type": ["array", "null"], "items": {"type": ["string"]}}

    def test_child_context_without_parent_context(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        assert stream.get_child_context({"id": "10000"}, None) is None

    def test_child_context_without_supports_options(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"fieldId": "customfield_10000"}
        assert stream.get_child_context({"id": "10000"}, context) is None

    def test_child_context_with_unsupported_options(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"fieldId": "customfield_10000", "supports_options": False}
        assert stream.get_child_context({"id": "10000"}, context) is None

    def test_child_context_missing_field_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"supports_options": True}
        assert stream.get_child_context({"id": "10000"}, context) is None

    def test_child_context_missing_record_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"fieldId": "customfield_10000", "supports_options": True}
        assert stream.get_child_context({}, context) is None

    def test_child_context_complete(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"fieldId": "customfield_10000", "supports_options": True}
        assert stream.get_child_context({"id": "10000"}, context) == {
            "fieldId": "customfield_10000",
            "contextId": "10000",
        }

    def test_post_process_without_parent_context(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        assert stream.post_process({"id": "10000"}, None) is None

    def test_post_process_without_field_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        assert stream.post_process({"id": "10000"}, {}) is None

    def test_post_process_global_context(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        row = {"id": "10000", "isGlobalContext": True}
        context = {"fieldId": "customfield_10000", "supports_options": True}

        record = stream.post_process(row, context)

        assert record is not None
        assert record["fieldId"] == "customfield_10000"
        assert record["projectIds"] == []

    def test_post_process_project_scoped_context(
        self,
        tap: TapJira,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stream = CustomFieldContextStream(tap)

        def fake_get_records(
            _self: CustomFieldContextProjectMappingStream,
            context: dict[str, t.Any] | None,
        ) -> Iterable[dict[str, t.Any]]:
            assert context == {
                "fieldId": "customfield_10000",
                "contextId": "10000",
            }
            records: list[dict[str, t.Any]] = [
                {"contextId": "10000", "projectId": "20001"},
                {"contextId": "10000", "projectId": "20002"},
                # a mapping for another context must not leak in
                {"contextId": "10001", "projectId": "20003"},
                # the global context mapping carries no project ID
                {"contextId": "10000", "isGlobalContext": True},
            ]
            return iter(records)

        monkeypatch.setattr(
            CustomFieldContextProjectMappingStream,
            "get_records",
            fake_get_records,
        )

        row = {"id": "10000", "isGlobalContext": False}
        context = {"fieldId": "customfield_10000", "supports_options": True}

        record = stream.post_process(row, context)

        assert record is not None
        assert record["fieldId"] == "customfield_10000"
        assert record["projectIds"] == ["20001", "20002"]

    def test_post_process_unresolvable_projects_not_guessed(
        self,
        tap: TapJira,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stream = CustomFieldContextStream(tap)

        def fail_get_records(
            _self: CustomFieldContextProjectMappingStream,
            _context: dict[str, t.Any] | None,
        ) -> Iterable[dict[str, t.Any]]:
            msg = "404 Client Error: Not Found"
            raise requests.HTTPError(msg)

        monkeypatch.setattr(
            CustomFieldContextProjectMappingStream,
            "get_records",
            fail_get_records,
        )

        row = {"id": "10000", "isGlobalContext": False}
        context = {"fieldId": "customfield_10000", "supports_options": True}

        record = stream.post_process(row, context)

        assert record is not None
        assert record["projectIds"] == []

    def test_post_process_context_without_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextStream(tap)
        context = {"fieldId": "customfield_10000", "supports_options": True}

        record = stream.post_process({"isGlobalContext": False}, context)

        assert record is not None
        assert record["projectIds"] == []


class TestCustomFieldOptionStream:
    """Tests for CustomFieldOptionStream."""

    def test_primary_keys_are_composite(self, tap: TapJira) -> None:
        stream = CustomFieldOptionStream(tap)
        assert stream.primary_keys == ("fieldId", "contextId", "id")

    def test_post_process_attaches_composite_key_parts(
        self,
        tap: TapJira,
    ) -> None:
        stream = CustomFieldOptionStream(tap)
        row = {"id": "30000", "value": "In Progress", "disabled": False}
        context = {"fieldId": "customfield_10000", "contextId": "10000"}

        record = stream.post_process(row, context)

        assert record is not None
        assert record["fieldId"] == "customfield_10000"
        assert record["contextId"] == "10000"
        assert record["id"] == "30000"

    def test_post_process_without_parent_context(self, tap: TapJira) -> None:
        stream = CustomFieldOptionStream(tap)
        assert stream.post_process({"id": "30000"}, None) is None

    def test_post_process_without_field_id(self, tap: TapJira) -> None:
        stream = CustomFieldOptionStream(tap)
        assert stream.post_process({"id": "30000"}, {"contextId": "10000"}) is None

    def test_post_process_without_context_id(self, tap: TapJira) -> None:
        stream = CustomFieldOptionStream(tap)
        row = {"id": "30000"}
        context = {"fieldId": "customfield_10000"}
        assert stream.post_process(row, context) is None


class TestCustomFieldContextProjectMappingStream:
    """Tests for the internal context project mapping helper stream."""

    def test_url_params_include_context_id_filter(self, tap: TapJira) -> None:
        stream = CustomFieldContextProjectMappingStream(tap)
        context = {"fieldId": "customfield_10000", "contextId": "10000"}

        params = stream.get_url_params(context, None)

        assert params["contextId"] == "10000"

    def test_url_params_without_context_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextProjectMappingStream(tap)

        params = stream.get_url_params({"fieldId": "customfield_10000"}, None)

        assert "contextId" not in params

    def test_url_path_includes_field_id(self, tap: TapJira) -> None:
        stream = CustomFieldContextProjectMappingStream(tap)
        context = {"fieldId": "customfield_10000", "contextId": "10000"}

        url = stream.get_url(context)

        assert url == (
            "https://test.atlassian.net/rest/api/3"
            "/field/customfield_10000/context/projectmapping"
        )

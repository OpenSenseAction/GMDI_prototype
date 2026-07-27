"""Tests for the coloring annotation API endpoints."""

import json
import sys
from contextlib import contextmanager
from unittest.mock import Mock, patch, MagicMock

import pytest

# Ensure optional heavy imports won't fail at import time
sys.modules.setdefault("folium", Mock())


@pytest.fixture
def auth_client(monkeypatch):
    """Test client with login bypassed."""
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    import main as wm

    mock_user = Mock()
    mock_user.id = "demo_openmrg"

    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)
    wm.app.config["TESTING"] = True

    return wm.app.test_client()


class TestColoringAnnotationAPI:
    """Tests for /api/coloring-annotation endpoint."""

    def test_create_annotation_success(self, auth_client, monkeypatch):
        """Test POST /api/coloring-annotation creates annotation via Grafana API."""
        epoch_sec = 1753632000  # 2026-07-27 16:00:00 UTC
        
        # Mock requests.post response
        mock_resp = Mock()
        mock_resp.json.return_value = {"id": 42, "message": "Annotation added"}
        mock_resp.status_code = 200
        
        with patch('main.requests.post', return_value=mock_resp) as mock_post:
            resp = auth_client.post(
                "/api/coloring-annotation",
                data=json.dumps({"epochSec": epoch_sec}),
                content_type="application/json"
            )
            
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "created"
            assert data["id"] == 42
            
            # Verify request payload
            assert mock_post.called
            call_kwargs = mock_post.call_args.kwargs
            body = call_kwargs["json"]
            assert body["time"] == (epoch_sec - 3600) * 1000
            assert body["timeEnd"] == epoch_sec * 1000
            assert body["tags"] == ["cml-coloring-window"]

    def test_update_annotation_success(self, auth_client, monkeypatch):
        """Test POST /api/coloring-annotation updates existing annotation."""
        import requests

        epoch_sec = 1753635600  # 2026-07-27 17:00:00 UTC
        existing_id = 42

        mock_resp = Mock()
        mock_resp.json.return_value = {
            "id": existing_id,
            "message": "Annotation updated",
        }
        mock_resp.status_code = 200

        with patch("main.requests.patch", return_value=mock_resp) as mock_patch:
            resp = auth_client.post(
                "/api/coloring-annotation",
                data=json.dumps({"epochSec": epoch_sec, "id": existing_id}),
                content_type="application/json",
            )

            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "updated"
            assert data["id"] == existing_id

    def test_delete_annotation_success(self, auth_client, monkeypatch):
        """Test DELETE /api/coloring-annotation removes annotation."""
        ann_id = 42

        mock_resp = Mock()
        mock_resp.status_code = 200

        with patch("main.requests.delete", return_value=mock_resp) as mock_del:
            resp = auth_client.delete(
                "/api/coloring-annotation",
                data=json.dumps({"id": ann_id}),
                content_type="application/json",
            )

            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "deleted"

    def test_missing_epochsec_returns_400(self, auth_client, monkeypatch):
        """Test POST without epochSec returns 400 error."""
        resp = auth_client.post(
            "/api/coloring-annotation",
            data=json.dumps({}),
            content_type="application/json",
        )

        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data


class TestColoringAnnotationCleanup:
    """Tests for /api/coloring-annotation-cleanup endpoint."""
    
    def test_cleanup_removes_all_tagged_annotations(self, auth_client, monkeypatch):
        """Test POST /api/coloring-annotation-cleanup purges all cml-coloring-window annotations."""
        # Mock GET returning existing annotations
        mock_get_resp = Mock()
        mock_get_resp.json.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_get_resp.status_code = 200
        
        # Mock DELETE responses
        mock_del_resp = Mock()
        mock_del_resp.status_code = 200
        
        with patch('main.requests.get', return_value=mock_get_resp) as mock_get:
            with patch('main.requests.delete', return_value=mock_del_resp) as mock_del:
                resp = auth_client.post("/api/coloring-annotation-cleanup")
                
                assert resp.status_code == 200
                data = resp.get_json()
                assert data["status"] == "ok"
                
                # Verify GET was called with correct params
                mock_get.assert_called_once()
                params = mock_get.call_args.kwargs.get("params", {})
                assert params.get("tags") == "cml-coloring-window"
                
                # Verify 3 DELETEs were called
                assert mock_del.call_count == 3

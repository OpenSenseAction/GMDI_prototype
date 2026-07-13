import logging
from typing import Optional

logger = logging.getLogger(__name__)


class JWTAuth:
    """Manages JWT-based authentication for a DRF Simple JWT API.

    Holds the active access/refresh token pair and transparently refreshes or
    re-logs in when requests fail with HTTP 401.

    Usage::

        auth = JWTAuth(login_url="http://api/login/", refresh_url="http://api/refresh/",
                       username="user", password="pass")
        response = auth.get(client, "http://api/data/", params={"foo": "bar"})
    """

    def __init__(
        self,
        login_url: str,
        refresh_url: str,
        username: str,
        password: str,
    ):
        self._login_url = login_url
        self._refresh_url = refresh_url
        self._username = username
        self._password = password
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _login(self, client) -> None:
        logger.info("Logging in to %s", self._login_url)
        resp = client.post(
            self._login_url,
            json={"username": self._username, "password": self._password},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access"]
        self._refresh_token = data["refresh"]

    def _refresh(self, client) -> None:
        logger.debug("Refreshing access token at %s", self._refresh_url)
        resp = client.post(
            self._refresh_url, json={"refresh": self._refresh_token}
        )
        if resp.status_code == 401:
            # Refresh token itself has expired — fall back to full login
            logger.info("Refresh token expired, re-logging in")
            self._login(client)
            return
        resp.raise_for_status()
        self._access_token = resp.json()["access"]

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._access_token}"}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, client, url: str, params: Optional[dict] = None) -> object:
        """Perform an authenticated GET, handling 401 with auto-refresh.

        Tries up to three times:
        1. First attempt with the cached access token (logging in first if needed).
        2. On 401 → refresh the access token and retry.
        3. If still 401 → full re-login and final retry.
        """
        if self._access_token is None:
            self._login(client)

        for attempt in range(3):
            resp = client.get(url, params=params, headers=self._auth_headers())
            if resp.status_code == 401:
                if attempt == 0:
                    self._refresh(client)
                elif attempt == 1:
                    self._login(client)
                else:
                    resp.raise_for_status()
            else:
                resp.raise_for_status()
                return resp

        # Should be unreachable, but keeps type checkers happy
        resp.raise_for_status()  # type: ignore[union-attr]
        return resp

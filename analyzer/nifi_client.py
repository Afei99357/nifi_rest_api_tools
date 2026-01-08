"""
Minimal NiFi REST API Client

Lightweight client for processor usage analysis.
Only includes methods needed for listing processors and querying provenance.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class NiFiClientError(Exception):
    """Base exception for NiFi client errors."""
    pass


class NiFiAuthError(NiFiClientError):
    """Authentication-related errors."""
    pass


class NiFiNotFoundError(NiFiClientError):
    """Resource not found errors."""
    pass


class NiFiClient:
    """
    Minimal NiFi REST API Client

    Provides only the methods needed for processor usage analysis:
    - Connecting to NiFi with authentication
    - Listing processors in a process group
    - Querying provenance events with date range filtering

    Args:
        base_url: NiFi base URL (e.g., "https://localhost:8443/nifi")
        username: NiFi username for authentication
        password: NiFi password for authentication
        verify_ssl: Whether to verify SSL certificates (default: False)
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of retry attempts (default: 3)
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = False,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        """Initialize NiFi client with authentication."""
        # Normalize base URL
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/nifi"):
            self.base_url += "/nifi"

        self.api_url = f"{self.base_url}-api"
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # Create session with retry logic
        self.session = self._create_session(max_retries)

        # Auth token (will be populated on first request if needed)
        self._auth_token: Optional[str] = None

        # Authenticate immediately to catch auth errors early
        self._authenticate()

        logger.info(f"Initialized NiFiClient for {self.base_url}")

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry logic and connection pooling."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Disable SSL warnings if verify_ssl is False
        if not self.verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        return session

    def _authenticate(self) -> None:
        """
        Authenticate with NiFi and obtain access token if using token-based auth.

        IMPORTANT: Token request must NOT use the same session to avoid session
        state conflicts that cause 403 errors on subsequent requests.
        """
        try:
            # Try token-based authentication (NiFi 1.14+)
            token_url = f"{self.api_url}/access/token"
            logger.debug(f"Attempting token authentication at {token_url}")

            # IMPORTANT: Use requests.post (not session) to avoid session state conflicts
            response = requests.post(
                token_url,
                data={"username": self.username, "password": self.password},
                verify=self.verify_ssl,
                timeout=self.timeout,
            )

            logger.debug(f"Token auth response: {response.status_code}")

            if response.status_code == 201:
                self._auth_token = response.text
                self.session.headers.update({"Authorization": f"Bearer {self._auth_token}"})
                logger.info("Successfully authenticated with token-based auth")
                return
            elif response.status_code == 404:
                # Token endpoint not available, use basic auth
                logger.info("Token endpoint not available, using basic auth")
                self.session.auth = HTTPBasicAuth(self.username, self.password)
                return
            else:
                logger.warning(f"Token auth failed with status {response.status_code}")
                logger.warning("Falling back to basic auth")
                self.session.auth = HTTPBasicAuth(self.username, self.password)

        except requests.RequestException as e:
            logger.warning(f"Token authentication failed: {e}, falling back to basic auth")
            self.session.auth = HTTPBasicAuth(self.username, self.password)

    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Make authenticated request to NiFi API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (will be joined with api_url)
            **kwargs: Additional arguments passed to requests

        Returns:
            Response object

        Raises:
            NiFiAuthError: Authentication failed
            NiFiNotFoundError: Resource not found
            NiFiClientError: Other API errors
        """
        # Ensure we're authenticated
        if not self._auth_token and not self.session.auth:
            self._authenticate()

        url = urljoin(f"{self.api_url}/", endpoint.lstrip("/"))

        # Set defaults
        kwargs.setdefault("verify", self.verify_ssl)
        kwargs.setdefault("timeout", self.timeout)

        logger.debug(f"{method} {url}")

        try:
            response = self.session.request(method, url, **kwargs)

            # Handle common error codes
            if response.status_code == 401:
                # Try to re-authenticate once
                logger.warning("Received 401, attempting re-authentication")
                self._auth_token = None
                self.session.auth = None
                self._authenticate()

                # Retry the request
                response = self.session.request(method, url, **kwargs)
                if response.status_code == 401:
                    raise NiFiAuthError(f"Authentication failed: {response.text}")

            elif response.status_code == 404:
                raise NiFiNotFoundError(f"Resource not found: {url}")

            elif response.status_code >= 400:
                raise NiFiClientError(
                    f"API request failed: {response.status_code} - {response.text}"
                )

            response.raise_for_status()
            return response

        except requests.RequestException as e:
            if isinstance(e, requests.HTTPError):
                raise NiFiClientError(f"HTTP error: {e}") from e
            raise NiFiClientError(f"Request failed: {e}") from e

    def get_root_process_group_id(self) -> str:
        """Get the root process group ID."""
        response = self._request("GET", "/flow/process-groups/root")
        data = response.json()
        return data["processGroupFlow"]["id"]

    def get_process_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get process group details including all processors, connections, etc.

        Args:
            group_id: Process group ID (use 'root' for root group)

        Returns:
            Process group data including processors, connections, and child groups
        """
        response = self._request("GET", f"/flow/process-groups/{group_id}")
        return response.json()

    def list_processors(self, group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all processors in a process group (or root if not specified).

        Args:
            group_id: Process group ID (defaults to root)

        Returns:
            List of processor objects
        """
        if group_id is None:
            group_id = self.get_root_process_group_id()

        pg_data = self.get_process_group(group_id)
        processors = pg_data["processGroupFlow"]["flow"]["processors"]

        # Recursively get processors from child groups
        child_groups = pg_data["processGroupFlow"]["flow"]["processGroups"]
        for child in child_groups:
            processors.extend(self.list_processors(child["id"]))

        return processors

    def get_process_group_status(self, group_id: str) -> Dict[str, Any]:
        """
        Get live execution statistics for all processors in a process group.

        Args:
            group_id: Process group ID (use 'root' for root group)

        Returns:
            Process group status data with processor execution counts
        """
        response = self._request("GET", f"/flow/process-groups/{group_id}/status")
        return response.json()

    def get_processor_invocation_counts(self, group_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Extract invocation counts for all processors in a process group (recursive).

        Args:
            group_id: Process group ID

        Returns:
            Dictionary mapping processor ID to {name, type, invocations}
        """
        status_data = self.get_process_group_status(group_id)
        processor_stats = {}

        # Log the top-level keys for debugging
        logger.debug(f"Status data top-level keys: {list(status_data.keys())}")

        # Extract processor stats from current group
        pg_status = status_data.get("processGroupStatus", {})

        if not pg_status:
            logger.warning(f"No 'processGroupStatus' key in response for group {group_id}")
            logger.warning(f"Response structure: {status_data}")
            return processor_stats

        logger.debug(f"processGroupStatus keys: {list(pg_status.keys())}")

        # Check if we got processor status data
        proc_status_list = pg_status.get("processorStatus", [])
        logger.debug(f"Found {len(proc_status_list)} processors in current group {group_id[:8]}")

        # Log first processor structure if available
        if proc_status_list and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"First processor status keys: {list(proc_status_list[0].keys())}")

        for proc_status in proc_status_list:
            try:
                proc_id = proc_status["id"]
                proc_name = proc_status["name"]
                proc_type = proc_status["type"].split('.')[-1]
                invocations = proc_status.get("aggregateSnapshot", {}).get("invocations", 0)

                processor_stats[proc_id] = {
                    "name": proc_name,
                    "type": proc_type,
                    "invocations": invocations
                }
                logger.debug(f"  Processor: {proc_name} - {invocations} invocations")
            except KeyError as e:
                logger.error(f"Missing key in processor status: {e}")
                logger.error(f"Processor status structure: {proc_status}")

        # Recursively get from child process groups
        child_groups = pg_status.get("processGroupStatus", [])
        logger.debug(f"Found {len(child_groups)} child process groups in {group_id[:8]}")

        for child_pg_status in child_groups:
            try:
                child_id = child_pg_status["id"]
                child_name = child_pg_status.get("name", "unknown")
                logger.debug(f"Recursing into child group: {child_name} ({child_id[:8]})")
                child_stats = self.get_processor_invocation_counts(child_id)
                processor_stats.update(child_stats)
                logger.debug(f"Added {len(child_stats)} processors from child group {child_name}")
            except Exception as e:
                logger.error(f"Error processing child group: {e}")

        logger.info(f"Group {group_id[:8]}: collected {len(processor_stats)} total processor stats")
        return processor_stats

    def get_processor_activity_from_connections(self, group_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Extract processor activity by aggregating connection flowfile counts.

        This method gets connection statistics from the Status API and aggregates
        them by source processor to calculate total flowfiles processed.

        Args:
            group_id: Process group ID

        Returns:
            Dictionary mapping processor name to {flowFilesOut, bytesOut}

        Example:
            {
                "PutHDFS": {"flowFilesOut": 1250, "bytesOut": 52000},
                "FetchSFTP": {"flowFilesOut": 105, "bytesOut": 8000}
            }
        """
        status_data = self.get_process_group_status(group_id)
        processor_activity = {}

        pg_status = status_data.get("processGroupStatus", {})
        if not pg_status:
            logger.warning(f"No 'processGroupStatus' key in response for group {group_id}")
            return processor_activity

        # Get all connections from current group
        connections = pg_status.get("aggregateSnapshot", {}).get("connectionStatusSnapshots", [])
        logger.debug(f"Found {len(connections)} connections in group {group_id[:8]}")

        # Aggregate by source processor
        for conn in connections:
            conn_snap = conn.get("connectionStatusSnapshot", {})
            source = conn_snap.get("sourceName")

            if source:
                # Initialize if first time seeing this processor
                if source not in processor_activity:
                    processor_activity[source] = {
                        "flowFilesOut": 0,
                        "bytesOut": 0
                    }

                # Aggregate outbound metrics
                processor_activity[source]["flowFilesOut"] += conn_snap.get("flowFilesOut", 0)
                processor_activity[source]["bytesOut"] += conn_snap.get("bytesOut", 0)

        logger.debug(f"Aggregated activity for {len(processor_activity)} processors")

        # Recurse into child groups
        child_groups = pg_status.get("processGroupStatus", [])
        logger.debug(f"Found {len(child_groups)} child groups in {group_id[:8]}")

        for child_pg in child_groups:
            try:
                child_id = child_pg["id"]
                child_name = child_pg.get("name", "unknown")
                logger.debug(f"Recursing into child group: {child_name} ({child_id[:8]})")
                child_activity = self.get_processor_activity_from_connections(child_id)

                # Merge child results
                for proc_name, activity in child_activity.items():
                    if proc_name not in processor_activity:
                        processor_activity[proc_name] = activity
                    else:
                        processor_activity[proc_name]["flowFilesOut"] += activity["flowFilesOut"]
                        processor_activity[proc_name]["bytesOut"] += activity["bytesOut"]

                logger.debug(f"Added {len(child_activity)} processors from child group {child_name}")
            except Exception as e:
                logger.error(f"Error processing child group: {e}")

        logger.info(f"Group {group_id[:8]}: collected activity for {len(processor_activity)} processors")
        return processor_activity

    def query_provenance(
        self,
        processor_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: int = 1000,
        max_events: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query provenance events with automatic pagination.

        Args:
            processor_id: Filter by processor ID (optional)
            start_date: Start date for query (optional)
            end_date: End date for query (optional)
            max_results: Results per page (default 1000, min 200 to avoid NiFi bug)
            max_events: Maximum total events to collect across all pages (optional)

        Returns:
            List of provenance events (may span multiple pages)
        """
        # Ensure max_results is at least 200 to avoid NiFi bug
        if max_results < 200:
            logger.warning(f"max_results={max_results} too low (NiFi bug), using 1000")
            max_results = 1000

        # If max_events not specified, just do a single query
        if max_events is None:
            return self._query_provenance_single(
                processor_id=processor_id,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results
            )

        # Paginate to collect up to max_events
        all_events = []
        current_end_date = end_date
        page_num = 1

        while len(all_events) < max_events:
            logger.debug(f"Fetching provenance page {page_num} (have {len(all_events)} events so far)")

            events = self._query_provenance_single(
                processor_id=processor_id,
                start_date=start_date,
                end_date=current_end_date,
                max_results=max_results
            )

            if not events:
                logger.debug("No more events available")
                break

            all_events.extend(events)
            page_num += 1

            # If we got fewer events than requested, we're done
            if len(events) < max_results:
                logger.debug(f"Got {len(events)} < {max_results}, last page reached")
                break

            # Use the oldest event's timestamp as end_date for next page
            oldest_event = events[-1]
            if "eventTime" in oldest_event:
                event_time_str = oldest_event["eventTime"]
                try:
                    # Remove timezone suffix for parsing
                    time_part = event_time_str.rsplit(" ", 1)[0]
                    current_end_date = datetime.strptime(time_part, "%m/%d/%Y %H:%M:%S.%f")
                    logger.debug(f"Next page will end at: {current_end_date}")
                except Exception as e:
                    logger.warning(f"Failed to parse event time '{event_time_str}': {e}")
                    break
            else:
                logger.debug("No eventTime in oldest event, stopping pagination")
                break

        result = all_events[:max_events] if max_events else all_events
        logger.info(f"Collected {len(result)} total provenance events across {page_num} pages")
        return result

    def _query_provenance_single(
        self,
        processor_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Execute a single provenance query (internal method)."""
        # Build query request
        query_request: Dict[str, Any] = {
            "provenance": {
                "request": {
                    "maxResults": max_results,
                }
            }
        }

        # Add dates DIRECTLY to request (NOT in searchTerms!)
        if start_date:
            query_request["provenance"]["request"]["startDate"] = start_date.strftime("%m/%d/%Y %H:%M:%S UTC")

        if end_date:
            query_request["provenance"]["request"]["endDate"] = end_date.strftime("%m/%d/%Y %H:%M:%S UTC")

        # Add component ID DIRECTLY to request (NOT in searchTerms!)
        if processor_id:
            query_request["provenance"]["request"]["componentId"] = processor_id

        # Submit query
        response = self._request("POST", "/provenance", json=query_request)
        query_data = response.json()
        query_id = query_data["provenance"]["id"]
        query_url = query_data["provenance"]["uri"]

        # Poll for results
        max_attempts = 30
        for attempt in range(max_attempts):
            time.sleep(1)  # Wait before polling

            response = self._request("GET", query_url.replace(self.api_url, ""))
            result = response.json()

            if result["provenance"]["finished"]:
                events = result["provenance"]["results"]["provenanceEvents"]
                logger.info(f"Retrieved {len(events)} provenance events")

                # Clean up query (CRITICAL: prevents "poorly behaving clients" error)
                try:
                    self._request("DELETE", f"/provenance/{query_id}")
                    logger.debug(f"Cleaned up provenance query {query_id}")
                except Exception as e:
                    logger.warning(f"Failed to clean up provenance query {query_id}: {e}")

                return events

            logger.debug(f"Waiting for provenance query (attempt {attempt + 1}/{max_attempts})")

        # Clean up timed-out query
        try:
            self._request("DELETE", f"/provenance/{query_id}")
            logger.debug(f"Cleaned up timed-out provenance query {query_id}")
        except Exception as e:
            logger.warning(f"Failed to clean up timed-out query {query_id}: {e}")

        raise NiFiClientError(f"Provenance query timed out after {max_attempts} attempts")

    def close(self) -> None:
        """Close the session and cleanup resources."""
        if self.session:
            self.session.close()
            logger.info("Closed NiFi client session")

    def __enter__(self) -> "NiFiClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

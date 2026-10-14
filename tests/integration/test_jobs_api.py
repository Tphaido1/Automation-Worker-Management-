import uuid

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_create_and_get_job(async_client: AsyncClient) -> None:
    """Verifies creating a job via POST /jobs and retrieving it via GET /jobs/{id}."""
    create_payload = {
        "type": "process_image",
        "payload": {"image_url": "https://example.com/pic.jpg", "filter": "grayscale"},
        "priority": 5,
        "max_retries": 4,
        "timeout_seconds": 120,
    }

    # 1. Create job
    create_resp = await async_client.post("/jobs", json=create_payload)
    assert create_resp.status_code == 201
    created_job = create_resp.json()

    job_id = created_job["id"]
    assert created_job["type"] == "process_image"
    assert created_job["payload"] == create_payload["payload"]
    assert created_job["status"] == "PENDING"
    assert created_job["priority"] == 5
    assert created_job["attempt"] == 0
    assert created_job["fencing_token"] == 0

    # 2. Retrieve job by ID
    get_resp = await async_client.get(f"/jobs/{job_id}")
    assert get_resp.status_code == 200
    retrieved_job = get_resp.json()

    assert retrieved_job["id"] == job_id
    assert retrieved_job["status"] == "PENDING"


@pytest.mark.asyncio
async def test_idempotent_job_creation(async_client: AsyncClient) -> None:
    """
    Verifies that creating jobs with the same (type, idempotency_key):
    1. Returns HTTP 201 on initial creation.
    2. Returns HTTP 200 with the exact same job record on subsequent requests.
    3. Does NOT create duplicate records in the database.
    """
    key = f"order-checkout-{uuid.uuid4()}"
    payload = {
        "type": "charge_customer",
        "payload": {"amount": 5000, "currency": "USD"},
        "idempotency_key": key,
    }

    # First request: new job created -> 201
    resp1 = await async_client.post("/jobs", json=payload)
    assert resp1.status_code == 201
    job1 = resp1.json()

    # Second request with identical type and key -> 200 OK (idempotent replay)
    resp2 = await async_client.post("/jobs", json=payload)
    assert resp2.status_code == 200
    job2 = resp2.json()

    # Verify identity
    assert job1["id"] == job2["id"]
    assert job1["created_at"] == job2["created_at"]
    assert job2["idempotency_key"] == key


@pytest.mark.asyncio
async def test_cancel_job_lifecycle(async_client: AsyncClient) -> None:
    """
    Verifies job cancellation:
    1. Active non-terminal job (PENDING) can be cancelled successfully.
    2. Attempting to cancel an already CANCELLED job fails with HTTP 400.
    """
    # 1. Create a job
    create_resp = await async_client.post(
        "/jobs",
        json={"type": "send_newsletter", "payload": {"subject": "Weekly Update"}},
    )
    job_id = create_resp.json()["id"]

    # 2. Cancel the job -> 200 OK
    cancel_resp = await async_client.post(f"/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    cancelled_job = cancel_resp.json()
    assert cancelled_job["status"] == "CANCELLED"
    assert cancelled_job["lease_owner"] is None

    # 3. Verify status persisted in DB
    get_resp = await async_client.get(f"/jobs/{job_id}")
    assert get_resp.json()["status"] == "CANCELLED"

    # 4. Attempt to cancel again -> 400 Bad Request (terminal state rule)
    repeat_cancel_resp = await async_client.post(f"/jobs/{job_id}/cancel")
    assert repeat_cancel_resp.status_code == 400
    err_body = repeat_cancel_resp.json()
    assert "already in terminal state" in err_body["detail"]


@pytest.mark.asyncio
async def test_get_nonexistent_job(async_client: AsyncClient) -> None:
    """Verifies querying an unknown job UUID returns 404."""
    fake_id = uuid.uuid4()
    resp = await async_client.get(f"/jobs/{fake_id}")
    assert resp.status_code == 404
    assert f"Job with ID '{fake_id}' not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_cancel_nonexistent_job(async_client: AsyncClient) -> None:
    """Verifies cancelling an unknown job UUID returns 404."""
    fake_id = uuid.uuid4()
    resp = await async_client.post(f"/jobs/{fake_id}/cancel")
    assert resp.status_code == 404
    assert f"Job with ID '{fake_id}' not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_get_job_executions_empty(async_client: AsyncClient) -> None:
    """Verifies /jobs/{id}/executions returns empty list when no execution attempts exist yet."""
    create_resp = await async_client.post(
        "/jobs",
        json={"type": "background_sync", "payload": {}},
    )
    job_id = create_resp.json()["id"]

    exec_resp = await async_client.get(f"/jobs/{job_id}/executions")
    assert exec_resp.status_code == 200
    assert exec_resp.json() == []

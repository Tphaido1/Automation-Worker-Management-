import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SentEmail:
    to: str
    subject: str
    body: str
    dedup_key: str


class FakeSMTPServer:
    """
    Simulated external SMTP server with built-in deduplication tracking (ADR-5).
    Tracks total delivery attempts vs actual unique emails delivered based on dedup_key.
    """

    def __init__(self) -> None:
        self.total_attempts: int = 0
        self.delivery_count: int = 0
        self.delivered_emails: list[SentEmail] = []
        self.attempts_per_dedup_key: dict[str, int] = defaultdict(int)
        self._seen_keys: set[str] = set()

    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        dedup_key: str,
    ) -> bool:
        """
        Attempts to deliver an email.
        If dedup_key has already been delivered, records duplicate attempt but suppresses duplicate delivery.
        Returns True if newly delivered, False if suppressed as a duplicate.
        """
        self.total_attempts += 1
        self.attempts_per_dedup_key[dedup_key] += 1

        if dedup_key in self._seen_keys:
            logger.warning(
                "FakeSMTPServer: duplicate email detected for dedup_key '%s'; suppressing delivery",
                dedup_key,
            )
            return False

        self._seen_keys.add(dedup_key)
        self.delivery_count += 1
        sent = SentEmail(to=to, subject=subject, body=body, dedup_key=dedup_key)
        self.delivered_emails.append(sent)
        logger.info(
            "FakeSMTPServer: delivered email to '%s' (dedup_key='%s', total_delivered=%d)",
            to,
            dedup_key,
            self.delivery_count,
        )
        return True

    def get_emails_for_recipient(self, to: str) -> list[SentEmail]:
        """Returns all delivered emails sent to a specific recipient."""
        return [e for e in self.delivered_emails if e.to == to]

    def reset(self) -> None:
        """Resets server state."""
        self.total_attempts = 0
        self.delivery_count = 0
        self.delivered_emails.clear()
        self.attempts_per_dedup_key.clear()
        self._seen_keys.clear()


def make_email_outbox_handler(
    smtp: FakeSMTPServer,
) -> Callable[[dict[str, Any], str], Awaitable[None]]:
    """
    Creates an outbox event handler for 'email' kind events
    that dispatches to the FakeSMTPServer.
    """

    async def handle_email_outbox(payload: dict[str, Any], dedup_key: str) -> None:
        to = payload["to"]
        subject = payload.get("subject", "No Subject")
        body = payload.get("body", "")
        smtp.send_email(to=to, subject=subject, body=body, dedup_key=dedup_key)

    return handle_email_outbox

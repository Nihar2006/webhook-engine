"""add_dead_letter_status

Revision ID: b2f4a8c91d05
Revises: 945c362c33e0
Create Date: 2026-09-29 14:25:00.000000

Adds the DEAD_LETTER value to the PostgreSQL ``eventstatus`` enum.

PostgreSQL enum notes
---------------------
* ``ALTER TYPE … ADD VALUE`` is the only way to extend a native enum.
  It cannot run inside a transaction on Postgres < 12; on PG 12+ it can.
  We use ``IF NOT EXISTS`` so the migration is idempotent (safe to re-run).

* Removing enum values from a PostgreSQL type is NOT supported.
  The downgrade path converts any DEAD_LETTER rows back to FAILED and
  leaves the enum value intact (documented limitation).
"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2f4a8c91d05"
down_revision: Union[str, Sequence[str], None] = "945c362c33e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add DEAD_LETTER to the eventstatus enum."""
    # IF NOT EXISTS makes this idempotent — safe to run multiple times.
    op.execute("ALTER TYPE eventstatus ADD VALUE IF NOT EXISTS 'DEAD_LETTER'")


def downgrade() -> None:
    """
    Postgres does not support removing enum values.

    We convert any DEAD_LETTER events back to FAILED so downstream code
    that does not know DEAD_LETTER continues to work.  The enum value
    itself is left in the type (removing it would require recreating the
    entire type and all columns that reference it).
    """
    op.execute(
        "UPDATE event SET status = 'FAILED' WHERE status = 'DEAD_LETTER'"
    )

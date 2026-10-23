"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-10-23
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geography

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

POINT_EXPR = "ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "geozones",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("radius_m", sa.Float(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("center", Geography("POINT", srid=4326, spatial_index=False), sa.Computed(POINT_EXPR, persisted=True)),
        sa.Column(
            "bounds",
            Geography(srid=4326, spatial_index=False),
            sa.Computed(f"ST_Buffer({POINT_EXPR}, radius_m)", persisted=True),
        ),
        sa.CheckConstraint("radius_m > 0", name="ck_geozones_radius_positive"),
        sa.CheckConstraint("latitude BETWEEN -90 AND 90", name="ck_geozones_latitude"),
        sa.CheckConstraint("longitude BETWEEN -180 AND 180", name="ck_geozones_longitude"),
    )
    op.create_index("ix_geozones_user_id_is_active", "geozones", ["user_id", "is_active"])
    op.create_index("ix_geozones_bounds", "geozones", ["bounds"], postgresql_using="gist")

    op.create_table(
        "devices",
        sa.Column("device_id", sa.String(64), primary_key=True),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column(
            "last_position", Geography("POINT", srid=4326, spatial_index=False), sa.Computed(POINT_EXPR, persisted=True)
        ),
    )
    op.create_index("ix_devices_last_position", "devices", ["last_position"], postgresql_using="gist")

    op.create_table(
        "device_locations",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("device_id", sa.String(64), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("position", Geography("POINT", srid=4326, spatial_index=False), sa.Computed(POINT_EXPR, persisted=True)),
    )
    op.create_index("ix_device_locations_device_id_recorded_at", "device_locations", ["device_id", "recorded_at"])

    op.create_table(
        "zone_presence",
        sa.Column("zone_id", sa.Uuid(), sa.ForeignKey("geozones.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("device_id", sa.String(64), primary_key=True),
        sa.Column("entered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_zone_presence_device_id", "zone_presence", ["device_id"])


def downgrade() -> None:
    op.drop_table("zone_presence")
    op.drop_table("device_locations")
    op.drop_table("devices")
    op.drop_table("geozones")

# app/models.py
# -----------------------------------------------------------------------------
# Modèles SQLAlchemy : schéma DB + relations
# Ajouts :
# - Activity: activity_type, sport, max_vam_5m/15m/30m + *_start_ts
# - Nouvelles tables : ActivityVamPeak, ActivityZoneSlopeAgg, UserSettings, UserVamPR
# -----------------------------------------------------------------------------
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    BigInteger,
    ForeignKey,
    Text,
    DateTime,
    Float,
    UniqueConstraint,
    Date,
    JSON,
)
from sqlalchemy.orm import relationship

from app.database import Base


# --- User ---------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)

    # Profil
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    location = Column(String, nullable=True)
    profile_image_url = Column(String, nullable=True)

    send_to_strava = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Physio
    birthdate = Column(Date, nullable=True)
    sex = Column(String(10), nullable=True)
    max_heartrate = Column(Integer, nullable=True)
    height_cm = Column(Float, nullable=True)
    weight_kg = Column(Float, nullable=True)
    is_pro = Column(Boolean, nullable=False, default=False)

    # Source CGM préférée : None / "libre" / "dexcom"
    cgm_source = Column(String(20), nullable=True, default=None)

    # Relations
    strava_tokens = relationship("StravaToken", back_populates="user")
    libre_credentials = relationship("LibreCredentials", back_populates="user", uselist=False)
    dexcom_tokens = relationship("DexcomToken", back_populates="user")
    activities = relationship("Activity", back_populates="user")

    # NEW
    settings = relationship("UserSettings", back_populates="user", uselist=False)
    vam_prs = relationship("UserVamPR", back_populates="user")
    vam_peaks = relationship("ActivityVamPeak", back_populates="user")  # <-- ajout

    # glucose_points via backref sur GlucosePoint



class StravaToken(Base):
    __tablename__ = "strava_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    athlete_id = Column(BigInteger, index=True, nullable=True, unique=True)

    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    expires_at = Column(BigInteger, nullable=False)  # timestamp Unix

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="strava_tokens")


class LibreCredentials(Base):
    __tablename__ = "libre_credentials"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, unique=True)

    email = Column(String, nullable=False)
    password_encrypted = Column(Text, nullable=False)

    region = Column(String, default="fr", nullable=False)
    client_version = Column(String, default="4.16.0", nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="libre_credentials")


class DexcomToken(Base):
    __tablename__ = "dexcom_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    expires_at = Column(BigInteger, nullable=False)  # Unix

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="dexcom_tokens")


class Activity(Base):
    __tablename__ = "activities"
    __table_args__ = (
        UniqueConstraint("user_id", "strava_activity_id", name="uq_user_activity"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    athlete_id = Column(BigInteger, nullable=False)
    strava_activity_id = Column(BigInteger, index=True, nullable=False)
    name = Column(String, nullable=True)

    # NEW: type Strava brut + type normalisé
    activity_type = Column(String(50), nullable=True)   # ex: 'Run','TrailRun','Ride',...
    sport = Column(String(32), nullable=True)           # ex: 'run','ride','hike',...

    start_date = Column(DateTime, nullable=False)
    elapsed_time = Column(Integer, nullable=True)  # sec

    distance = Column(Float, nullable=True)
    total_elevation_gain = Column(Float, nullable=True)
    average_heartrate = Column(Float, nullable=True)

    difficulty_score = Column(Float, nullable=True)   # distance_km + D+/100
    level = Column(Integer, nullable=True)            # 1 à 5 (vert → or)

    # CGM
    avg_glucose = Column(Float, nullable=True)
    min_glucose = Column(Float, nullable=True)
    max_glucose = Column(Float, nullable=True)
    time_in_range_percent = Column(Float, nullable=True)
    hypo_count = Column(Integer, nullable=True)
    hyper_count = Column(Integer, nullable=True)
    glucose_summary_block = Column(Text, nullable=True)

    # NEW: caches VAM
    max_vam_5m = Column(Float, nullable=True)
    max_vam_15m = Column(Float, nullable=True)
    max_vam_30m = Column(Float, nullable=True)
    max_vam_5m_start_ts = Column(DateTime, nullable=True)
    max_vam_15m_start_ts = Column(DateTime, nullable=True)
    max_vam_30m_start_ts = Column(DateTime, nullable=True)

    last_synced_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="activities")

    stream_points = relationship(
        "ActivityStreamPoint",
        back_populates="activity",
        cascade="all, delete-orphan",
    )
    # NEW: relations agrégats
    vam_peaks = relationship(
        "ActivityVamPeak",
        back_populates="activity",
        cascade="all, delete-orphan",
    )
    zone_slope_aggs = relationship(
        "ActivityZoneSlopeAgg",
        back_populates="activity",
        cascade="all, delete-orphan",
    )


class ActivityStreamPoint(Base):
    __tablename__ = "activity_stream_points"

    id = Column(Integer, primary_key=True, index=True)

    activity_id = Column(
        Integer,
        ForeignKey("activities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    idx = Column(Integer, nullable=False)

    elapsed_time = Column(Integer, nullable=True)
    distance = Column(Float, nullable=True)
    altitude = Column(Float, nullable=True)
    heartrate = Column(Float, nullable=True)
    cadence = Column(Float, nullable=True)
    velocity = Column(Float, nullable=True)
    watts = Column(Float, nullable=True)
    grade = Column(Float, nullable=True)
    temp = Column(Float, nullable=True)
    moving = Column(Boolean, nullable=True)

    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)

    timestamp = Column(DateTime, nullable=True)

    glucose_mgdl = Column(Float, nullable=True)
    glucose_trend = Column(String, nullable=True)
    glucose_source = Column(String, nullable=True)

    slope_percent = Column(Float, nullable=True)
    vertical_speed_m_per_h = Column(Float, nullable=True)
    hr_zone = Column(String, nullable=True)

    activity = relationship("Activity", back_populates="stream_points")


# --- ActivityVamPeak ---------------------------------------------
class ActivityVamPeak(Base):
    __tablename__ = "activity_vam_peaks"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    activity_id = Column(Integer, ForeignKey("activities.id", ondelete="CASCADE"), nullable=False, index=True)

    sport = Column(String(32), nullable=False)  # normalisé
    window_min = Column(Integer, nullable=False)  # 5, 15, 30

    max_vam_m_per_h = Column(Float, nullable=False)
    start_idx = Column(Integer, nullable=False)
    end_idx = Column(Integer, nullable=False)
    start_ts = Column(DateTime, nullable=True)
    end_ts = Column(DateTime, nullable=True)
    gain_m = Column(Float, nullable=False)
    loss_m = Column(Float, nullable=False)
    loss_pct_vs_gain = Column(Float, nullable=False)
    distance_m = Column(Float, nullable=True)
    method = Column(String, nullable=True)

    activity = relationship("Activity", back_populates="vam_peaks")
    user = relationship("User", back_populates="vam_peaks")  # <-- corrigé



class ActivityZoneSlopeAgg(Base):
    __tablename__ = "activity_zone_slope_aggs"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    activity_id = Column(Integer, ForeignKey("activities.id", ondelete="CASCADE"), nullable=False, index=True)

    sport = Column(String(32), nullable=False)
    hr_zone = Column(String, nullable=False)       # 'Zone 1'...'Zone 5'
    slope_band = Column(String, nullable=False)    # 'S0_5','S5_10','S10_15','S15_20','S20_25','S25_30','S30_40','S40p'

    duration_sec = Column(Integer, nullable=False)
    num_points = Column(Integer, nullable=False)
    distance_m = Column(Float, nullable=True)
    elevation_gain_m = Column(Float, nullable=True)

    avg_vam_m_per_h = Column(Float, nullable=True)
    avg_cadence_spm = Column(Float, nullable=True)
    avg_velocity_m_s = Column(Float, nullable=True)
    avg_pace_s_per_km = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("activity_id", "hr_zone", "slope_band"),
    )

    activity = relationship("Activity", back_populates="zone_slope_aggs")


class RunnerProfileMonthly(Base):
    __tablename__ = "runner_profile_monthly"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "sport",
            "year_month",
            "metric_scope",
            "slope_band",
            "hr_zone",
            "fatigue_bucket",
            "window_label",
            name="uq_runner_profile_monthly_cell",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    sport = Column(String(32), nullable=False)
    year_month = Column(Date, nullable=False)
    metric_scope = Column(String(32), nullable=False)

    slope_band = Column(String(32), nullable=True)
    hr_zone = Column(String(32), nullable=True)
    fatigue_bucket = Column(String(32), nullable=True)
    window_label = Column(String(32), nullable=True)

    total_duration_sec = Column(Float, nullable=False, default=0.0)
    total_distance_m = Column(Float, nullable=False, default=0.0)
    total_elevation_gain_m = Column(Float, nullable=False, default=0.0)
    total_points = Column(Integer, nullable=False, default=0)

    sum_pace_x_duration = Column(Float, nullable=False, default=0.0)
    pace_duration_sec = Column(Float, nullable=False, default=0.0)

    sum_vam_x_duration = Column(Float, nullable=False, default=0.0)
    vam_duration_sec = Column(Float, nullable=False, default=0.0)

    sum_cadence_x_duration = Column(Float, nullable=False, default=0.0)
    cadence_duration_sec = Column(Float, nullable=False, default=0.0)

    sum_velocity_x_duration = Column(Float, nullable=False, default=0.0)
    velocity_duration_sec = Column(Float, nullable=False, default=0.0)

    avg_pace_s_per_km = Column(Float, nullable=True)
    avg_vam_m_per_h = Column(Float, nullable=True)
    avg_cadence_spm = Column(Float, nullable=True)
    avg_velocity_m_s = Column(Float, nullable=True)

    dplus_total_m = Column(Float, nullable=True)
    dminus_total_m = Column(Float, nullable=True)

    extra = Column(JSON, nullable=True)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User")


class UserSettings(Base):
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, unique=True)

    desc_include_glycemia = Column(Boolean, default=True)
    desc_include_vam = Column(Boolean, default=False)
    desc_include_pace = Column(Boolean, default=False)
    desc_include_cadence = Column(Boolean, default=False)
    desc_enable_auto_block = Column(Boolean, default=True)

    desc_format = Column(String(32), default="gly_first")
    desc_max_lines = Column(Integer, nullable=True)

    user = relationship("User", back_populates="settings")


class UserVamPR(Base):
    __tablename__ = "user_vam_prs"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    sport = Column(String(32), nullable=False)           # 'run','ride',...
    window_min = Column(Integer, nullable=False)         # 5, 15, 30
    vam_m_per_h = Column(Float, nullable=False)
    activity_id = Column(Integer, ForeignKey("activities.id"), nullable=True, index=True)
    start_ts = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "sport", "window_min", name="uq_user_sport_win"),
    )

    user = relationship("User", back_populates="vam_prs")


class GlucosePoint(Base):
    __tablename__ = "glucose_points"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    ts = Column(DateTime, nullable=False, index=True)  # UTC naïf
    mgdl = Column(Float, nullable=False)
    trend = Column(String, nullable=True)

    source = Column(String, nullable=False, default="realtime")

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "ts", name="uq_glucose_user_ts"),
    )

    user = relationship("User", backref="glucose_points")

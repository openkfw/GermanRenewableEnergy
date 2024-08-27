from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    ARRAY,
    UniqueConstraint, PrimaryKeyConstraint,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass

class ResultBase:

    EinheitMastrNummer = Column(String, primary_key=True)
    year = Column(Integer, primary_key=True)
    software_version = Column(String)
    outfile_postfix = Column(String)
    no_calc_reason = Column(String)


class ResultsWindHourly(ResultBase, Base):
    __tablename__ = "results_wind_hourly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_hourly"
        ),
    )

    cf_h = Column(ARRAY(Float))
    energy_h = Column(ARRAY(Float))


class ResultsWindMonthly(ResultBase, Base):
    __tablename__ = "results_wind_monthly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_monthly"
        ),
    )

    cf_m = Column(ARRAY(Float))
    energy_m = Column(ARRAY(Float))


class ResultsWindYearly(ResultBase, Base):
    __tablename__ = "results_wind_yearly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_yearly"
        ),
    )

    cf_y = Column(Float)
    energy_y = Column(Float)


class ResultsSolarHourly(ResultBase, Base):
    __tablename__ = "results_solar_hourly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_hourly_solar"
        ),
    )

    cf_h = Column(ARRAY(Float))
    energy_h = Column(ARRAY(Float))


class ResultsSolarMonthly(ResultBase, Base):
    __tablename__ = "results_solar_monthly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_monthly_solar"
        ),
    )

    cf_m = Column(ARRAY(Float))
    energy_m = Column(ARRAY(Float))


class ResultsSolarYearly(ResultBase, Base):
    __tablename__ = "results_solar_yearly"
    __table_args__ = (
        UniqueConstraint(
            "EinheitMastrNummer", "year", name="unique_mastrid_year_yearly_solar"
        ),
    )

    cf_y = Column(Float)
    energy_y = Column(Float)


class unique_era5_coordinates(Base):
    __tablename__ = "unique_era5_coordinates"

    coordinate_id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)


class CalcBase(object):

    EinheitMastrNummer = Column(String, primary_key=True)
    EinheitBetriebsstatus = Column(String)
    Nettonennleistung = Column(Float)
    Breitengrad = Column(Float)
    Laengengrad = Column(Float)
    Gemeindeschluessel = Column(String)
    Inbetriebnahmedatum = Column(Date)
    ags_lat = Column(Float)
    ags_lon = Column(Float)
    era5_ags_lat = Column(Float)
    era5_ags_lon = Column(Float)
    Postleitzahl = Column(String)
    plz_lat = Column(Float)
    plz_lon = Column(Float)
    era5_plz_lat = Column(Float)
    era5_plz_lon = Column(Float)
    mapping_log = Column(String)


class Calculation_wind(CalcBase, Base):
    __tablename__ = "Calculation_wind"

    Nabenhoehe = Column(Float)
    Typenbezeichnung = Column(String)
    turbine_mapped = Column(String)
    hub_height_mapped = Column(Float)


class Calculation_solar(CalcBase, Base):
    __tablename__ = "Calculation_solar"

    Hauptausrichtung = Column(String)
    HauptausrichtungNeigungswinkel = Column(String)
    azimuth_angle_mapped = Column(Float)
    tilt_angle_mapped = Column(Float)

class Calculation_solar_angles(Base):
    __tablename__ = "Calculation_solar_angles"

    year = Column(Integer)
    lat_lon = Column(String)
    solar_zenith = Column(ARRAY(Float))
    solar_azimuth = Column(ARRAY(Float))
    era5_lat = Column(Float)
    era5_lon = Column(Float)


    __table_args__ = (
            PrimaryKeyConstraint('era5_lat', 'era5_lon', 'year'),
        )

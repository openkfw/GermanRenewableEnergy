import os
from typing import List, Optional

import psycopg2
import psycopg2.extras

from kfw_mastr.utils.config import setup_logger
from kfw_mastr.utils.orm import ResultsSolarMonthly, ResultsSolarYearly, ResultsSolarHourly, ResultsWindMonthly, \
    ResultsWindYearly, ResultsWindHourly, Calculation_solar_angles

logger = setup_logger()


def query_table(
    session,
    table,
    column_names: List[str],
    limit: int = None,
    incremental: bool = False,
    year: int = None,
) -> List:
    """
    Query a database table for specified columns, limit the number of results, and filter by mastr IDs.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        The database session.
    table : sqlalchemy.Table
        The table to query.
    column_names : List[str]
        List of column names to retrieve.
    limit : int, optional
        The maximum number of records to retrieve, by default None.
    mastr_ids : List[str], optional
        List of specific mastr IDs to filter the query, by default None.
    incremental: bool, Default: False
        Incremental data processing is only available for solar. If this is swichted on,
        the programm
    year: int, optional, needed if incremental
       

    Returns
    -------
    List
        The query results as a list of tuples.
    """

    mastr_ids = []
    solar_angle_year = os.getenv("YEAR_SOLAR_ANGLE")
    # check for specific mastr units
    if table.__tablename__ == "Calculation_solar":
        # Check for specific solar units
        solar_units = os.getenv('SPECIFIC_SOLAR_UNITS')
        if solar_units and solar_units != "None":
            mastr_ids = [unit.strip() for unit in os.getenv('SPECIFIC_SOLAR_UNITS', '').split(",")]
            logger.info(f"Filter for solar mastr units: {mastr_ids}")

    if table.__tablename__ == "Calculation_wind":
        wind_units = os.getenv('SPECIFIC_WIND_UNITS')
        if wind_units and wind_units != "None":
            mastr_ids = [unit.strip() for unit in os.getenv('SPECIFIC_WIND_UNITS', '').split(",")]
            logger.info(f"Filter for wind mastr units: {mastr_ids}")

    # Convert column names to column attributes
    columns = [getattr(table, column_name) for column_name in column_names]
    query = session.query(*columns)

    if incremental:
        # find 
        # Alias für die Tabelle "ResultsSolarYearly"
        tab_result = aliased(ResultsSolarYearly)
        query = (query.outerjoin(tab_result, (table.EinheitMastrNummer == tab_result.EinheitMastrNummer) 
                                 & (tab_result.year == year)
                                ).filter(tab_result.EinheitMastrNummer.is_(None))
                )
        

    if mastr_ids:
        query = query.filter(table.EinheitMastrNummer.in_(mastr_ids))

    if solar_angle_year and table == Calculation_solar_angles:
        query = query.filter(table.year == solar_angle_year)

    if limit:
        query = query.limit(limit)
    logger.info(str(query))    

    return query.all()


def save_and_commit(
    session,
    updates: List[dict],
    conflict_action: str,
    key: str,
    batch_size: Optional[int] = None,
    table_id: str = None
) -> None:
    """
    Save updates to the database and commit the session.

    Parameters
    ----------
    session : Any
        The database session to use for committing the updates.
    updates : List[dict]
        A list of dictionaries containing the updates to be saved.
    conflict_action : str
        The action to take in case of a conflict during the save operation.
    key : str
        The key used to identify the updates.
    batch_size : Optional[int], optional
        The number of updates after which the session should be committed.
        If None, the session will be committed immediately (default is None).
    table_id : Optional[int], optional
        The ID of the table where the updates should be saved (default is None).

    Returns
    -------
    None
        This function does not return any value.
    """
    if batch_size is None or len(updates) >= batch_size:
        save_to_db(
            session,
            **{key: updates},
            conflict_action=conflict_action,
            table_id=table_id
        )
        updates.clear()
        session.commit()

def save_to_db(
    session,
    updates_hourly: list = None,
    updates_monthly: list = None,
    updates_yearly: list = None,
    conflict_action: str = None,
    table_id: str = None,
) -> None:
    """
    Save updates to the database for hourly, monthly, and yearly results.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        The database session.
    updates_hourly : list, optional
        List of hourly updates to save, by default None.
    updates_monthly : list, optional
        List of monthly updates to save, by default None.
    updates_yearly : list, optional
        List of yearly updates to save, by default None.
    conflict_action : str, optional
        Action to take on conflict, by default None.
    table_id : str, optional
        Identifier for the table type (e.g., "wind" or "solar").

    Returns
    -------
    None
    """

    table_mapping = {
        "wind": {
            "hourly": ResultsWindHourly,
            "monthly": ResultsWindMonthly,
            "yearly": ResultsWindYearly,
        },
        "solar": {
            "hourly": ResultsSolarHourly,
            "monthly": ResultsSolarMonthly,
            "yearly": ResultsSolarYearly,
        },
    }

    if table_id not in table_mapping:
        raise ValueError(f"Invalid table_id: {table_id}")

    updates = {
        "hourly": updates_hourly,
        "monthly": updates_monthly,
        "yearly": updates_yearly,
    }

    for time_aggregation, update_list in updates.items():
        if update_list:
            batch_update(
                session,
                table=table_mapping[table_id][time_aggregation],
                updates=update_list,
                id_cols=["EinheitMastrNummer", "year"],
                conflict_action=conflict_action,
            )



def batch_update(session, table, updates, id_cols, conflict_action="skip_existing_row"):
    """
    Commit updates in chunks using psycopg2.extras.execute_values and handle conflicts.

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        The database session.
    table : str
        The name of the table to update.
    updates : list
        List of dictionaries containing the updates.
    id_cols : list
        List of columns to identify rows for conflict resolution.
    conflict_action : str, optional
        Action to take on conflict, by default "skip_existing_row".
        When "update" is chosen, rows are updated.
        When "update_hard" is chosen, rows are updated using a join without unique constraints.

    Returns
    -------
    None
    """
    if not updates:
        return

    table_name = table.__tablename__
    schema = table.metadata.schema or "public"
    full_table_name = f'"{schema}"."{table_name}"'  # Quote the schema and table name
    conflict_cols = ", ".join([f'"{col}"' for col in id_cols])
    columns = updates[0].keys()
    quoted_columns = [f'"{col}"' for col in columns]  # Quote column names
    data = [tuple(update.values()) for update in updates]

    if conflict_action == "skip_existing_row":
        insert_query = f"""
        INSERT INTO {full_table_name} ({", ".join(quoted_columns)}) VALUES %s
        ON CONFLICT ({conflict_cols}) DO NOTHING
        """
    elif conflict_action == "update":
        insert_query = f"""
        INSERT INTO {full_table_name} ({", ".join(quoted_columns)}) VALUES %s
        ON CONFLICT ({conflict_cols}) DO UPDATE SET 
        {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in id_cols])}
        """
    elif conflict_action == "join":
        temp_table = "tmp_update_data"
        create_temp_table_query = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                Gemeindeschluessel TEXT,
                ags_lon DOUBLE PRECISION,
                ags_lat DOUBLE PRECISION,
                era5_ags_lat DOUBLE PRECISION,
                era5_ags_lon DOUBLE PRECISION
            )
            """
        insert_temp_query = f"""
            INSERT INTO {temp_table} (Gemeindeschluessel, ags_lon, ags_lat, era5_ags_lat, era5_ags_lon) VALUES %s
            """
        update_query = f"""
            UPDATE {full_table_name} AS target
            SET 
                ags_lat = temp.ags_lat,
                ags_lon = temp.ags_lon,
                era5_ags_lat = temp.era5_ags_lat,
                era5_ags_lon = temp.era5_ags_lon
            FROM {temp_table} AS temp
            WHERE target."Gemeindeschluessel" = temp.Gemeindeschluessel
            """
        drop_temp_table_query = "DROP TABLE tmp_update_data"

    connection = session.connection().connection
    cursor = connection.cursor()

    try:
        if conflict_action == "join":
            cursor.execute(create_temp_table_query)
            connection.commit()

            psycopg2.extras.execute_values(
                cursor, insert_temp_query, data, template=None, page_size=200
            )
            connection.commit()

            cursor.execute(update_query)
            connection.commit()

            cursor.execute(drop_temp_table_query)
            connection.commit()
        else:
            psycopg2.extras.execute_values(
                cursor, insert_query, data, template=None, page_size=200
            )
            connection.commit()
    except (psycopg2.errors.UndefinedColumn, psycopg2.errors.UniqueViolation) as e:
        print(f"Error: {e}")
        connection.rollback()
        raise
    except Exception as e:
        connection.rollback()
        raise
    finally:
        cursor.close()


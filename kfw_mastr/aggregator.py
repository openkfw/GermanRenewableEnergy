"""
This module aggregates the result of all calculation within kfwmastr to provide easier data consumption.

Note:
This module is without using sqlalchemy to make performance optimization easier

TODO:
  - consolidate aggregate_solar and aggregate_wind into one (same code...)
     ATTENTION: actually the development is working on agg_wind
  - even improve performance
"""

import os
import time
import csv
from typing import Dict

import numpy as np
from tqdm import tqdm

from kfw_mastr.utils.config import get_engine, setup_logger

# configs
logger = setup_logger()
engine, metadata = get_engine()


def delete_tmp_tables(cur):
  """
    Find and delete all tables with the prefix "tmp_"
  """
  # Find all tables with the prefix "tmp_"
  cur.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name LIKE 'tmp\_%' ESCAPE '\'
  """)

  tables_to_drop = cur.fetchall()

  # Drop all tables with the prefix "tmp_"
  for table in tables_to_drop:
    cur.execute("DROP TABLE IF EXISTS {} CASCADE;".format('.'.join(table)))
  
  # Commit the changes
  cur.connection.commit()


def output_table_count(cur):
  """
    output the count of the result tables (prefix "agg_")
  """
  # Get a list of all tables with the prefix "agg_"
  cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name LIKE 'agg\_%' ESCAPE '\'
  """)

  tables = cur.fetchall()

  # Output the count of rows in each table
  for table in tables:
    cur.execute("SELECT COUNT(*) FROM {};".format(table[0]))
    count = cur.fetchone()[0]
    print("Table {}: {}".format(table[0], count))


"""
Hauptausrichtung, HauptausrichtungNeigungswinkel | azimuth_angle_mapped | tilt_angle_mapped | EinheitMastrNummer | EinheitBetriebsstatus | Nettonennleistung Gemeindeschluessel, ags_lat, ags_lon, Inbetriebnahmedatum
"""

def aggregate_solar(debug=False):
    """
    TODO

    Parameters
    ----------
    None

    Returns
    -------
    None
    
    TODO
    -------
    - processing data for each year?
    - creating no rows for year < 2000?
    - creating always rows for each aggregation level / year-combination?
    - bl may implement as further aggregation of ags (but!: than the avg_cf is to be calculated by (avg_cf*anzahl)/(gesamt anzahl))
    """

    # Load all solar units and their "Stammdaten"
    logger.info(f"Aggregate solar units")

    # Write all table to a CSV file
    outpath = os.path.join(os.environ['OUTPUT_PATH'],  "solar")
    os.makedirs(outpath, exist_ok=True)   
    
    con = engine.raw_connection()
    with con.cursor() as cur:
      def drop_n_create(table, sql, pk = None):
        # just drops and create table using the given sql as select
        logger.info(f"create table {table} (with pk: {pk})")
        #print(sql)
        cur.execute(f"drop table if exists {table}");
        con.commit()
        cur.execute(f"create table {table} as {sql}")
        con.commit()
        if pk:
          cur.execute(f"ALTER TABLE {table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({pk});")
          con.commit()
            
     

      
      # creating tmp_solar_agg and tmp_solar_agg_year
      tab1, debug = "tmp_agg_solar_all_debug", True
      # tab1, debug = "tmp_agg_solar_all", False
      drop_n_create("tmp_agg_solar_all_res", f"""
          select   "EinheitMastrNummer" as EMastr, 
                   avg(cf_y)    as avg_cf -- # TODO: avg_cf_power_weighted
          from     results_solar_yearly 
          group by "EinheitMastrNummer" """, "EMastr")

      drop_n_create(tab1, f"""
        select    clc."EinheitMastrNummer",
                  clc."Postleitzahl"                               as plz,
                  clc."Nettonennleistung"    ,
                  plz_lat, plz_lon,
                  coalesce(clc."Gemeindeschluessel", 'MISSING')   as ags, -- WIND: case when "Bundesland" = Ausschließliche Wirtschaftszone" dann "Seelage",
                  clc.ags_lat, clc.ags_lon,
                  coalesce("Bundesland", 'MISSING')               as bl,
                  case when clc."Inbetriebnahmedatum" is not NULL then EXTRACT(YEAR FROM clc."Inbetriebnahmedatum") else -1 end as Inbetriebnahmejahr,
                  "DatumEndgueltigeStilllegung"                   as Stilllegung_datum,
                  case when "DatumEndgueltigeStilllegung" is not NULL then EXTRACT(YEAR FROM "DatumEndgueltigeStilllegung") else 9999 end as Stilllegung_jahr,
                  res.avg_cf
        from      "Calculation_solar"         clc
        left join solar_extended              ext
             on (clc."EinheitMastrNummer" = ext."EinheitMastrNummer") 
        left join tmp_agg_solar_all_res       res
             on (clc."EinheitMastrNummer" = res.EMastr) 
        """+(" limit 1000 " if debug else ""), '"EinheitMastrNummer"')
    
      # iterating over all aggregatins
      for agg_lvl, grp_cols in [("ags", ",ags_lat, ags_lon"),
                                ("plz", ",plz_lat, plz_lon"),
                                ("bl", "")
                               ]:
          logger.info(f"create aggregation on {agg_lvl}-level")

          drop_n_create("tmp_agg_solar", f"""
             select   {agg_lvl}{grp_cols},
                      avg(avg_cf)                                                       as avg_cf_gesamt,
                      case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf* "Nettonennleistung") / sum("Nettonennleistung")       
                        else Null end                                                   as avg_cf_gesamt_power_weighted,
                      sum("Nettonennleistung")                                          as nettonennleistung_gesamt,
                      count(*)                                                          as anzahl_gesamt
             from     {tab1}
             group by {agg_lvl}{grp_cols}""", agg_lvl)


          pk_cols = f"{agg_lvl}, Inbetriebnahmejahr"
          grp_cols2 = f"{pk_cols}{grp_cols}"
          drop_n_create("tmp_agg_year", f"""
            select   {grp_cols2}, 
                     sum("Nettonennleistung")       as nettonennleistung_inbetriebnahme,
                     count(*)                       as anzahl_inbetriebnahme
            from     {tab1}
            group by {grp_cols2}""", 
                        pk_cols)

          pk_cols = f"{agg_lvl}, Inbetriebnahmejahr"
          drop_n_create("tmp_agg_act_year_cf", f"""
          select     {pk_cols},
                     avg(cf_y)                                                      as act_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(cf_y * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as act_cf_y_power_weighted,
                     avg(avg_cf)                                                    as act_avg_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as act_avg_cf_y_power_weighted,
                     sum(energy_y)                                                  as act_energy_y
          from       {tab1}                pvs
          left join  results_solar_yearly  res
                 on (    pvs.Inbetriebnahmejahr = res.year
                     and pvs."EinheitMastrNummer" = res."EinheitMastrNummer")
          group by {pk_cols}
          """, pk_cols)
        
          pk_cols = f"{agg_lvl}, year"
          drop_n_create("tmp_agg_run_year_cf", f"""
          select     {pk_cols},
                     avg(cf_y)                                                      as run_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(cf_y * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as run_cf_y_power_weighted,
                     avg(avg_cf)                                                    as run_avg_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as run_avg_cf_y_power_weighted,
                     sum(energy_y)                                                  as run_energy_y
          from       {tab1}                pvs
          left join  results_solar_yearly  res
                 on (    pvs.Inbetriebnahmejahr <= res.year
                     and res.year < Stilllegung_jahr
                     and pvs."EinheitMastrNummer" = res."EinheitMastrNummer"
                     )
          where pvs.Inbetriebnahmejahr is not Null and res.year is not Null
          group by {pk_cols}
          """, pk_cols)
        
          pk_cols = f"{agg_lvl}"
          drop_n_create(f"agg_solar_{agg_lvl}", f"""
          select     'pv' as tech, *
          from       tmp_agg_solar       pvs
          """, pk_cols)

          pk_cols = f"tech, {agg_lvl},yr"
          cols = """act_cf_y,
                    act_cf_y_power_weighted,
                    act_avg_cf_y,
                    act_avg_cf_y_power_weighted,
                    act_energy_y,
                    run_cf_y,
                    run_cf_y_power_weighted,
                    run_avg_cf_y,
                    run_avg_cf_y_power_weighted,
                    run_energy_y"""
          drop_n_create(f"agg_solar_{agg_lvl}_yr", f"""
          select     'solar' as tech,
                     tmp_yr as yr,
                     pvs.*,   
                     -- nettonennleistung_inbetriebnahme,
                     -- anzahl_inbetriebnahme,
                     {cols}
          from       tmp_agg_solar      pvs
          left join  (select      coalesce(act.{agg_lvl}, run.{agg_lvl})      as tmp_{agg_lvl},
                                  coalesce(act.Inbetriebnahmejahr, run.year)  as tmp_yr,
                                  {cols}
                      from             tmp_agg_act_year_cf   act
                      full outer join  tmp_agg_run_year_cf   run
                               on     act.{agg_lvl} = run.{agg_lvl}
                                 and  act.Inbetriebnahmejahr = run.year
                      -- left join  tmp_agg_solar_year pv_yr
                      -- on ()
                     ) blub
                  on      pvs.{agg_lvl} = blub.tmp_{agg_lvl}
          """, pk_cols)

          for table in ["tmp_agg_solar", "tmp_agg_year", "tmp_agg_act_year_cf"]:
              cur.execute(f"drop table if exists {table}");
              con.commit()
         
          for table, pk in [(f"agg_solar_{agg_lvl}", agg_lvl), 
                            (f"agg_solar_{agg_lvl}_yr", f"{agg_lvl},yr"),
                       ]:
            filename = os.path.join(outpath, f'{table}.csv')
            logger.info(f"output {table} agg_solar_year to {filename}")
            cur.execute(f"SELECT * FROM {table} ORDER BY {pk}")
            # Fetch all rows from the result set
            rows = cur.fetchall()
            # Get the column names from the cursor description
            colnames = [desc[0] for desc in cur.description]
            
            with open(filename, 'w', newline='') as csvfile:
              csvwriter = csv.writer(csvfile)
              csvwriter.writerow(colnames)  # Write the column headers
              csvwriter.writerows(rows)  # Write the data rows


def aggregate(tech, debug=False):
    """
    TODO

    Parameters
    ----------
    tech:str - has to be 'solar' or 'wind'

    Returns
    -------
    None
    
    TODO
    -------
    - processing data for each year?
    - creating no rows for year < 2000?
    - creating always rows for each aggregation level / year-combination?
    - bl may implement as further aggregation of ags (but!: than the avg_cf is to be calculated by (avg_cf*anzahl)/(gesamt anzahl))
    """
    if tech not in ['solar', 'wind']:
      raise ValueError("parameter tech has to be 'solar' or 'wind'")

    # Load all wind or solar units and their "Stammdaten"
    logger.info(f"Aggregate {tech} units")

    # Write all table to a CSV file
    outpath = os.path.join(os.environ['OUTPUT_PATH'],  tech)
    os.makedirs(outpath, exist_ok=True)   
    
    con = engine.raw_connection()
    with con.cursor() as cur:

      def drop_n_create(table, sql, pk = None):
        """
          helper function to drop and create table using a the give sql
        """
        # just drops and create table using the given sql as select
        logger.info(f"create table {table} (with pk: {pk})")
    
        cur.execute(f"drop table if exists {table}");
        con.commit()
        cur.execute(f"create table {table} as {sql}")
        con.commit()
        if pk:
            cur.execute(f"ALTER TABLE {table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({pk});")
            con.commit()

      # creating tmp_{tech}_agg and tmp_{tech}_agg_year
      tab1 = f"tmp_agg_{tech}_all_debug" if debug else f"tmp_agg_{tech}_all"
      drop_n_create(f"tmp_agg_{tech}_all_res", f"""
          select   "EinheitMastrNummer" as EMastr, 
                   avg(cf_y)    as avg_cf -- TODO: avg_cf_power_weighted
          from     results_{tech}_yearly 
          group by "EinheitMastrNummer" """, "EMastr")
      drop_n_create(tab1, f"""
        select    clc."EinheitMastrNummer",
                  coalesce(clc."Postleitzahl", '') as plz,
                  clc."Nettonennleistung"    ,
                  plz_lat, plz_lon,
                  coalesce(clc."Gemeindeschluessel", 'MISSING')   as ags, -- WIND: case when "Bundesland" = Ausschließliche Wirtschaftszone" dann "Seelage",
                  clc.ags_lat, clc.ags_lon,
                  coalesce("Bundesland", 'MISSING')               as bl,
                  case when clc."Inbetriebnahmedatum" is not NULL then EXTRACT(YEAR FROM clc."Inbetriebnahmedatum") else -1 end as Inbetriebnahmejahr,
                  "DatumEndgueltigeStilllegung"                   as Stilllegung_datum,
                  case when "DatumEndgueltigeStilllegung" is not NULL then EXTRACT(YEAR FROM "DatumEndgueltigeStilllegung") else 9999 end as Stilllegung_jahr,
                  res.avg_cf
        from      "Calculation_{tech}"         clc
        left join {tech}_extended              ext
             on (clc."EinheitMastrNummer" = ext."EinheitMastrNummer") 
        left join tmp_agg_{tech}_all_res       res
             on (clc."EinheitMastrNummer" = res.EMastr) 
        """+(" limit 1000 " if debug else ""), '"EinheitMastrNummer"')
    
      # iterating over all aggregatins
      for agg_lvl, grp_cols in [("ags", ",ags_lat, ags_lon"),
                                ("plz", ",plz_lat, plz_lon"),
                                ("bl", "")
                               ]:
          logger.info(f"create aggregation on {agg_lvl}-level")

          drop_n_create(f"tmp_agg_{tech}", f"""
             select   {agg_lvl}{grp_cols},
                      avg(avg_cf)                                                       as avg_cf_gesamt,
                      case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf* "Nettonennleistung") / sum("Nettonennleistung")       
                        else Null end                                                   as avg_cf_gesamt_power_weighted,
                      sum("Nettonennleistung")                                          as nettonennleistung_gesamt,
                      count(*)                                                          as anzahl_gesamt
             from     {tab1}
             group by {agg_lvl}{grp_cols}""", agg_lvl)


          pk_cols = f"{agg_lvl}, Inbetriebnahmejahr"
          grp_cols2 = f"{pk_cols}{grp_cols}"
          drop_n_create("tmp_agg_year", f"""
            select   {grp_cols2}, 
                     sum("Nettonennleistung")       as nettonennleistung_inbetriebnahme,
                     count(*)                       as anzahl_inbetriebnahme
            from     {tab1}
            group by {grp_cols2}""", 
                        pk_cols)

          pk_cols = f"{agg_lvl}, Inbetriebnahmejahr"
          drop_n_create("tmp_agg_act_year_cf", f"""
          select     {pk_cols},
                     avg(cf_y)                                                      as act_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(cf_y * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as act_cf_y_power_weighted,
                     avg(avg_cf)                                                    as act_avg_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as act_avg_cf_y_power_weighted,
                     sum(energy_y)                                                  as act_energy_y
          from       {tab1}                pvs
          left join  results_{tech}_yearly  res
                 on (    pvs.Inbetriebnahmejahr = res.year
                     and pvs."EinheitMastrNummer" = res."EinheitMastrNummer")
          group by {pk_cols}
          """, pk_cols)
        
          pk_cols = f"{agg_lvl}, year"
          drop_n_create("tmp_agg_run_year_cf", f"""
          select     {pk_cols},
                     avg(cf_y)                                                      as run_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(cf_y * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as run_cf_y_power_weighted,
                     avg(avg_cf)                                                    as run_avg_cf_y,
                     case when sum("Nettonennleistung") > 0 
                        then sum(avg_cf * "Nettonennleistung") / sum("Nettonennleistung")
                        else Null end                                               as run_avg_cf_y_power_weighted,
                     sum(energy_y)                                                  as run_energy_y
          from       {tab1}                pvs
          left join  results_{tech}_yearly  res
                 on (    pvs.Inbetriebnahmejahr <= res.year
                     and res.year < Stilllegung_jahr
                     and pvs."EinheitMastrNummer" = res."EinheitMastrNummer"
                     )
          where pvs.Inbetriebnahmejahr is not Null and res.year is not Null
          group by {pk_cols}
          """, pk_cols)
        
          pk_cols = f"{agg_lvl}"
          drop_n_create(f"agg_{tech}_{agg_lvl}", f"""
          select     '{tech}' as tech, *
          from       tmp_agg_{tech}       pvs
          """, pk_cols)

          pk_cols = f"tech, {agg_lvl},yr"
          cols = """act_cf_y,
                    act_cf_y_power_weighted,
                    act_avg_cf_y,
                    act_avg_cf_y_power_weighted,
                    act_energy_y,
                    run_cf_y,
                    run_cf_y_power_weighted,
                    run_avg_cf_y,
                    run_avg_cf_y_power_weighted,
                    run_energy_y"""
          drop_n_create(f"agg_{tech}_{agg_lvl}_yr", f"""
          select     '{tech}' as tech,
                     tmp_yr as yr,
                     pvs.*,   
                     -- nettonennleistung_inbetriebnahme,
                     -- anzahl_inbetriebnahme,
                     {cols}
          from       tmp_agg_{tech}    pvs
          left join  (select      coalesce(act.{agg_lvl}, run.{agg_lvl})      as tmp_{agg_lvl},
                                  coalesce(act.Inbetriebnahmejahr, run.year)  as tmp_yr,
                                  {cols}
                      from             tmp_agg_act_year_cf   act
                      full outer join  tmp_agg_run_year_cf   run
                               on     act.{agg_lvl} = run.{agg_lvl}
                                 and  act.Inbetriebnahmejahr = run.year
                      -- left join  tmp_agg_{tech}_year pv_yr
                      -- on ()
                     ) blub
                  on      pvs.{agg_lvl} = blub.tmp_{agg_lvl}
          """, pk_cols)

          for table, pk in [(f"agg_{tech}_{agg_lvl}", agg_lvl), 
                            (f"agg_{tech}_{agg_lvl}_yr", f"{agg_lvl},yr"),
                       ]:
            filename = os.path.join(outpath, f'{table}.csv')
            logger.info(f"output {table} agg_wind_year to {filename}")
            cur.execute(f"SELECT * FROM {table} ORDER BY {pk}")
            # Fetch all rows from the result set
            rows = cur.fetchall()
            # Get the column names from the cursor description
            colnames = [desc[0] for desc in cur.description]
            
            with open(filename, 'w', newline='') as csvfile:
              csvwriter = csv.writer(csvfile)
              csvwriter.writerow(colnames)  # Write the column headers
              csvwriter.writerows(rows)  # Write the data rows

      output_table_count(cur)
      #delete_tmp_tables(cur) 


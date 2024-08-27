"""
  Testing the complete calculation chain.

  !!!STATUS: Blind coded Pseudo-Code!!!
"""

import json
import os


def test_calculate_cf_wka()
  """
    Testing the complete calculation chain for a selection of wka and only for the first two hours of an year.

    !!!STATUS: Blind coded Pseudo-Code!!!

    The test works in the following steps:
      1) Check test environment
      2) Define exeptected result
      3) Define the configuration for the run 
      4) Run the calculate_cf
      5) Check against expected result
  """

  ###########################################
  # 1) Check test environment
  ###########################################
  # at least, we need a path to find anything...
  assert "KFW_MASTR_PATH" in os.environ.keys() 
  test_cfg_filename = os.environ["KFW_MASTR_PATH"]+/test/tmp_f

  ###########################################
  # 2) Define exeptected result
  ###########################################
  # at least, we need a path to find anything...
  expected_results = {
    "MASTRNUMMER_WKA_EINHEIT_WITH_DEFAULT_CURVE_AND_NO_LON_LAT4AGS_FOUND": {  
          # thus, 
          # fsr - forecast_surface_roughness
          # v_h0 - wind speed 100 / 10m (pytagoras of u&v wind component)
          # hh - hub height
          "wka-lon": "[TODO]", # find also those wkas as test cases, where wka-lon coudnt be derived using the AGS
          "wka-lat": "[TODO]",
          "era5-lon: "[TODO]",
          "era5-lat: "[TODO]",
          "hh": "[TODO]",
          "years": { 2020: { # --> vv
                             # sp - surface pressure@2
                                # v_wind = np.sqrt(u**2 + v**2)
                                # v_hh = v_h0 * np.log(hh / fsr) / np.log(h0 / fsr)
                                # p_hh = sp * (1 - 0.0065 * hh / t2m) ** 5.255
                                # t_hh = t2m - (6.5 * hh / t2m)
                                # rho_hh = p_hh / (t_hh * R)
                                # v_hh_norm = v_hh * (rho_hh / RHO_0) ** (1 / 3)
                             "cf":      "TODO", 
                             "power":  "TODO", 
                 }
          }
    },
    "MASTRNUMMER_WKA_EINHEIT_WITH_GIVEN_CURVE_AND_AGS_DERIVED_LON/LAT": {  
    },
    "OFFSHORE...": {  
    },
    "FRUTHER INTERESTING CASES...": {  
    },
  }

  ###########################################
  # 3) Define the configuration for the run 
  ###########################################
  test_cfg = {
    "...": "... all the other necessary settings",
    "years_start": 2022,
    "years_end": 2023,
    "mastr_list": list(test_mastrnr.keys()), # run only for 
    "debug_mode": 3 # thus, run calculation only for the first two hours of each year!
  }
  with open(test_cfg_filename, 'w') as f:
     json.dump(test_cfg, f)

  ###########################################
  # 4) Run the calculate_cf
  ###########################################
  # TODO: calculate_cf: define a def calculate_cf::main() and the 'if __name__ == "__main__"':-code block is only an a call of def main()
  calculate_cf.main() 

  ###########################################
  # 5) Check against expected result
  ###########################################
  for exp_mastr, exp_result in expected_results.items():
      # TODO: load the result of the mastr
      print("Loading result data to ", mastr)
      result = {} # todo load the caculated result 
      
      for key, exp_value in exp_result:
        print("Checking excpected result ", key, " is ", value)
        # TODO: Iterate through each expected value and check it against the calculated calte
        # ...

        if key == "years":
          for y, y_dict in value.items():
            # TODO: Iterate through each expected value and check it against the calculated calte
            # ...
        else:
          assert result[key] = exp_value
        


def test_calculate_cf_pv():
    # as def test_calculate_cf_wka() just for PV

#!/usr/bin/env python3
"""Cycles Executor."""

import argparse
import csv
import logging
import os
import shutil
import subprocess
import sys
from string import Template

log = logging.getLogger()

global basedir
basedir = os.path.dirname(__file__)

KILL_CROP, FERTILIZE, TILLAGE1, PLANT_CROP, TILLAGE2, WEED = range(6)

def _adjust_doy(doy):

    doy = doy - 365 if doy > 365 else doy
    doy = doy + 365 if doy < 1 else doy

    return doy

def _get_doy(op):

    return op['doy']


def _op_str(op):

    if op['type'] == KILL_CROP:
        strs = 'TILLAGE\n'
        strs += '%-20s%d\n' %('YEAR', 1)
        strs += '%-20s%d\n' %('DOY', op['doy'])
        strs += '%-20s%s\n' %('TOOL', 'Kill_Crop')
        strs += '%-20s%s\n' %('DEPTH', '0')
        strs += '%-20s%s\n' %('SOIL_DISTURB_RATIO', '0')
        strs += '%-20s%s\n' %('MIXING_EFFICIENCY', '0')
        strs += '%-20s%s\n' %('CROP_NAME', 'N/A')
        strs += '%-20s%s\n' %('FRAC_THERMAL_TIME', '0.0')
        strs += '%-20s%s\n' %('KILL_EFFICIENCY', '0.0')
        strs += '%-20s%s\n' %('GRAIN_HARVEST', '0')
        strs += '%-20s%s\n\n' %('FORAGE_HARVEST', '0.0')
        return strs
    elif op['type'] == FERTILIZE:
        strs= 'FIXED_FERTILIZATION\n'
        strs += '%-20s%s\n' % ('YEAR', '1')
        strs += '%-20s%s\n' % ('DOY', op['doy'])
        strs += '%-20s%s\n' % ('SOURCE', '32-00-00')
        strs += '%-20s%s\n' % ('MASS', op['fertilizer_rate'])
        strs += '%-20s%s\n' % ('FORM', 'Solid')
        strs += '%-20s%s\n' % ('METHOD', 'Incorporated')
        strs += '%-20s%s\n' % ('LAYER', '2')
        strs += '%-20s%s\n' % ('C_Organic', '0')
        strs += '%-20s%s\n' % ('C_Charcoal', '0')
        strs += '%-20s%s\n' % ('N_Organic', '0')
        strs += '%-20s%s\n' % ('N_Charcoal', '0')
        strs += '%-20s%s\n' % ('N_NH4', '1')
        strs += '%-20s%s\n' % ('N_NO3', '0')
        strs += '%-20s%s\n' % ('P_Organic', '0')
        strs += '%-20s%s\n' % ('P_CHARCOAL', '0')
        strs += '%-20s%s\n' % ('P_INORGANIC', '0')
        strs += '%-20s%s\n' % ('K', '0')
        strs += '%-20s%s\n\n' % ('S', '0')
        return strs
    elif op['type'] == TILLAGE1:
        strs = 'TILLAGE\n'
        strs += '%-20s%s\n' %('YEAR', '1')
        strs += '%-20s%d\n' %('DOY', op['doy'])
        strs += '%-20s%s\n' %('TOOL', 'Hand_hoeing')
        strs += '%-20s%s\n' %('DEPTH', '0.11')
        strs += '%-20s%s\n' %('SOIL_DISTURB_RATIO', '25')
        strs += '%-20s%s\n' %('MIXING_EFFICIENCY', '0.33')
        strs += '%-20s%s\n' %('CROP_NAME', 'N/A')
        strs += '%-20s%s\n' %('FRAC_THERMAL_TIME', '0.0')
        strs += '%-20s%s\n' %('KILL_EFFICIENCY', '0.0')
        strs += '%-20s%s\n' %('GRAIN_HARVEST', '0')
        strs += '%-20s%s\n\n' %('FORAGE_HARVEST', '0.0')
        return strs
    elif op['type'] == PLANT_CROP:
        strs = 'PLANTING\n'
        strs += '%-20s%s\n' % ('YEAR', '1')
        strs += '%-20s%d\n' % ('DOY', op['doy'])
        strs += '%-20s%s\n' % ('END_DOY', op['end_planting_date'])
        strs += '%-20s%s\n' % ('MAX_SMC', '-999')
        strs += '%-20s%s\n' % ('MIN_SMC', '-999')
        strs += '%-20s%s\n' % ('MIN_SOIL_TEMP', '-999')
        strs += '%-20s%s\n' % ('CROP', op['crop'])
        strs += '%-20s%s\n' % ('USE_AUTO_IRR', '0')
        strs += '%-20s%s\n' % ('USE_AUTO_FERT', '0')
        strs += '%-20s%s\n' % ('FRACTION', '0.67')
        strs += '%-20s%s\n' % ('CLIPPING_START', '1')
        strs += '%-20s%s\n\n' % ('CLIPPING_END', '366')
        return strs
    elif op['type'] == TILLAGE2:
        strs = 'TILLAGE\n'
        strs += '%-20s%s\n' %('YEAR', '1')
        strs += '%-20s%d\n' %('DOY', op['doy'])
        strs += '%-20s%s\n' %('TOOL', 'Hand_hoeing_weeding')
        strs += '%-20s%s\n' %('DEPTH', '0.06')
        strs += '%-20s%s\n' %('SOIL_DISTURB_RATIO', '15')
        strs += '%-20s%s\n' %('MIXING_EFFICIENCY', '0.25')
        strs += '%-20s%s\n' %('CROP_NAME', 'N/A')
        strs += '%-20s%s\n' %('FRAC_THERMAL_TIME', '0.0')
        strs += '%-20s%s\n' %('KILL_EFFICIENCY', '0.0')
        strs += '%-20s%s\n' %('GRAIN_HARVEST', '0')
        strs += '%-20s%s\n\n' %('FORAGE_HARVEST', '0.0')
        return strs
    elif op['type'] == WEED:
        if float(op['weed_fraction']) > 0:
            strs = 'PLANTING\n'
            strs += '%-20s%s\n' % ('YEAR', '1')
            strs += '%-20s%d\n' % ('DOY', op['doy'])
            strs += '%-20s%s\n' % ('END_DOY', '-999')
            strs += '%-20s%s\n' % ('MAX_SMC', '-999')
            strs += '%-20s%s\n' % ('MIN_SMC', '-999')
            strs += '%-20s%s\n' % ('MIN_SOIL_TEMP', '-999')
            strs += '%-20s%s\n' % ('CROP', 'C3_weed')
            strs += '%-20s%s\n' % ('USE_AUTO_IRR', '0')
            strs += '%-20s%s\n' % ('USE_AUTO_FERT', '0')
            strs += '%-20s%s\n' % ('FRACTION', op['weed_fraction'])
            strs += '%-20s%s\n' % ('CLIPPING_START', '-999')
            strs += '%-20s%s\n\n' % ('CLIPPING_END', '-999')
            strs = 'PLANTING\n'
            strs += '%-20s%s\n' % ('YEAR', '1')
            strs += '%-20s%d\n' % ('DOY', op['doy'])
            strs += '%-20s%s\n' % ('END_DOY', '-999')
            strs += '%-20s%s\n' % ('MAX_SMC', '-999')
            strs += '%-20s%s\n' % ('MIN_SMC', '-999')
            strs += '%-20s%s\n' % ('MIN_SOIL_TEMP', '-999')
            strs += '%-20s%s\n' % ('CROP', 'C4_weed')
            strs += '%-20s%s\n' % ('USE_AUTO_IRR', '0')
            strs += '%-20s%s\n' % ('USE_AUTO_FERT', '0')
            strs += '%-20s%s\n' % ('FRACTION', op['weed_fraction'])
            strs += '%-20s%s\n' % ('CLIPPING_START', '-999')
            strs += '%-20s%s\n\n' % ('CLIPPING_END', '-999')
        else:
            strs = ''
        return strs

def _generate_inputs(
        start_year,
        end_year,
        crop,
        start_planting_date,
        end_planting_date,
        fertilizer_rate,
        weed_fraction,
        forcing,
        weather_file,
        reinit_file,
        crop_file,
        soil_file,
        **kwargs):

    global basedir
    ctrl_file = "cycles-run.ctrl"
    op_file = "cycles-run.operation"

    # process CTRL file
    with open(f"{basedir}/template.ctrl") as t_ctrl_file:
        src = Template(t_ctrl_file.read())
        ctrl_data = {
            "start_year": start_year,
            "end_year": end_year,
            "rotation_size": 1,
            "crop_file": crop_file,
            "operation_file": op_file,
            "soil_file": soil_file,
            "weather_file": weather_file,
            "reinit": 0,
        }
        result = src.substitute(ctrl_data)
        with open("./input/" + ctrl_file, "w") as f:
            f.write(result)

    # process Operation file
    ## set operation days and other parameters
    ops = [
        {'type': KILL_CROP, 'doy': _adjust_doy(int(start_planting_date) - 10)},
        {'type': FERTILIZE, 'doy': _adjust_doy(int(start_planting_date) - 10), 'fertilizer_rate': fertilizer_rate},
        {'type': TILLAGE1, 'doy': _adjust_doy(int(start_planting_date) - 10)},
        {'type': PLANT_CROP, 'doy': _adjust_doy(int(start_planting_date)), 'crop': crop, 'end_planting_date': end_planting_date},
        {'type': TILLAGE2, 'doy': _adjust_doy(int(start_planting_date) + 20)},
        {'type': WEED, 'doy': _adjust_doy(int(start_planting_date) + 7), "weed_fraction": weed_fraction},
    ]

    ## sort all operations by days of year
    ops.sort(key=_get_doy)

    operation_contents = ""

    for op in ops:
        operation_contents += _op_str(op)

    # writing operations file
    with open("./input/" + op_file, "w") as f:
        f.write(operation_contents)


def _launch(**kwargs):
    cmd = f"{basedir}/Cycles -s cycles-run"
    print(cmd)
    try:
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
        exit(1)
    else:
        print("Output: \n{}\n".format(output))


def _main():
    parser = argparse.ArgumentParser(
        description="Cycles executor."
    )
    parser.add_argument("--start-year", dest="start_year", default=2000, help="Simulation start year")
    parser.add_argument("--end-year", dest="end_year", default=2017, help="Simulation end year")
    parser.add_argument("-c", "--crop", dest="crop", default="Maize", help="Crop name")
    parser.add_argument("-s", "--start-planting-date", dest="start_planting_date", default=100, help="Start planting date")
    parser.add_argument("-e", "--end-planting-date", dest="end_planting_date", default=0, help="End planting date") # Changed default to 0, which changes to -999 below, and implies automatic end planting date
    parser.add_argument("-n", "--fertilizer-rate", dest="fertilizer_rate", default=0.00, help="Fertilizer rate")
    parser.add_argument("-w", "--weed-fraction", dest="weed_fraction", default=0.0, help="Weed fraction")
    parser.add_argument("-f", "--forcing", dest="forcing", default=False, help="Whether it uses forcing data from PIHM")
    parser.add_argument("-l", "--weather-file", dest="weather_file", default=None, help="Weather file")
    parser.add_argument("-r", "--reinit-file", dest="reinit_file", default=None, help="Cycles reinitialization file")
    parser.add_argument("crop_file", help="crops file")
    parser.add_argument("soil_file", help="Soil file")
    args = parser.parse_args()

    if args.end_planting_date == 0:
        args.end_planting_date = -999

    _generate_inputs(**vars(args))
    _launch(**vars(args))


if __name__ == "__main__":
    _main()

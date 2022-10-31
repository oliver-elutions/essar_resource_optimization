#!/bin/bash

echo "Starting Furnace A"
python main.py ./Data/furnace_a_val.csv ../opt_a_001.csv 0.01 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl
python main.py ./Data/furnace_a_val.csv ../opt_a_01.csv 0.1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl
python main.py ./Data/furnace_a_val.csv ../opt_a_1.csv 1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl
python main.py ./Data/furnace_a_val.csv ../opt_a_10.csv 10 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl
python main.py ./Data/furnace_a_val.csv ../opt_a_100.csv 100 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl
python main.py ./Data/furnace_a_val.csv ../opt_a_1000.csv 1000 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_a.pkl

echo "Starting Furnace B"
python main.py ./Data/furnace_b_val.csv ../opt_b_001.csv 0.01 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl
python main.py ./Data/furnace_b_val.csv ../opt_b_01.csv 0.1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl
python main.py ./Data/furnace_b_val.csv ../opt_b_1.csv 1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl
python main.py ./Data/furnace_b_val.csv ../opt_b_10.csv 10 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl
python main.py ./Data/furnace_b_val.csv ../opt_b_100.csv 100 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl
python main.py ./Data/furnace_b_val.csv ../opt_b_1000.csv 1000 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_b.pkl

echo "Starting Furnace C"
python main.py ./Data/furnace_c_val.csv ../opt_c_001.csv 0.01 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl
python main.py ./Data/furnace_c_val.csv ../opt_c_01.csv 0.1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl
python main.py ./Data/furnace_c_val.csv ../opt_c_1.csv 1 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl
python main.py ./Data/furnace_c_val.csv ../opt_c_10.csv 10 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl
python main.py ./Data/furnace_c_val.csv ../opt_c_100.csv 100 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl
python main.py ./Data/furnace_c_val.csv ../opt_c_1000.csv 1000 --config-path=./essar_controllable_a_b_c.json --model-path=./Data/rf_furnace_c.pkl

echo "Starting Furnace D"
python main.py ./Data/furnace_d_val.csv ../opt_d_001.csv 0.01 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
python main.py ./Data/furnace_d_val.csv ../opt_d_01.csv 0.1 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
python main.py ./Data/furnace_d_val.csv ../opt_d_1.csv 1 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
python main.py ./Data/furnace_d_val.csv ../opt_d_10.csv 10 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
python main.py ./Data/furnace_d_val.csv ../opt_d_100.csv 100 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
python main.py ./Data/furnace_d_val.csv ../opt_d_1000.csv 1000 --config-path=./essar_controllable_d.json --model-path=./Data/rf_furnace_d.pkl
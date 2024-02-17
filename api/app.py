from flask import Flask, render_template, jsonify
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen
import os

app = Flask(__name__)

def generate_vehicle_data_once():
    process = Popen(['python3', 'generator_vehicle.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_viajes'])
    process.wait()

def generate_user_data_once():
    process = Popen(['python3', 'generador_usuarios2.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_clientes'])
    process.wait()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_vehicle_data', methods=['POST'])
def generate_vehicle_data():
    num_generations_v = 1 

    with ThreadPoolExecutor(max_workers=num_generations_v) as executor:
        executor.map(lambda _: generate_vehicle_data_once(), range(num_generations_v))

    return jsonify({"status": "success", "message": f"Generating Vehicles {num_generations_v} times in parallel."})

@app.route('/generate_user_data', methods=['POST'])
def generate_user_data():
    num_generations_u = 1  

    with ThreadPoolExecutor(max_workers=num_generations_u) as executor:
        executor.map(lambda _: generate_user_data_once(), range(num_generations_u))

    return jsonify({"status": "success", "message": f"Generating Users {num_generations_u} times in parallel."})
     
@app.route('/blablacar_racing', methods=['POST'])
def run_videogame():
    blablacar_racing_path = os.path.abspath('../blablacar_racing')

    if os.path.exists(blablacar_racing_path):
        os.chdir(blablacar_racing_path)

        process = Popen(['python3', 'main.py'])
        
        return jsonify({"status": "success", "message": "Cambiando tu vida"})
    else:
        return jsonify({"status": "error", "message": "Path to blablacar_racing folder not found."})  
     
if __name__ == '__main__':
    app.run(debug=False)

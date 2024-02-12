from flask import Flask, render_template, jsonify
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen

app = Flask(__name__)

def generate_vehicle_data_once():
    process = Popen(['python3', 'generator_vehicle.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_viajes'])
    process.wait()

def generate_user_data_once():
    process = Popen(['python3', 'generador_usuarios.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_clientes'])
    process.wait()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_vehicle_data', methods=['POST'])
def generate_vehicle_data():
    num_generations_v = 20 

    with ThreadPoolExecutor(max_workers=num_generations_v) as executor:
        executor.map(lambda _: generate_vehicle_data_once(), range(num_generations_v))

    return jsonify({"status": "success", "message": f"Generating Vehicles {num_generations_v} times in parallel."})

@app.route('/generate_user_data', methods=['POST'])
def generate_user_data():
    num_generations_u = 20  

    with ThreadPoolExecutor(max_workers=num_generations_u) as executor:
        executor.map(lambda _: generate_user_data_once(), range(num_generations_u))

    return jsonify({"status": "success", "message": f"Generating Users {num_generations_u} times in parallel."})

if __name__ == '__main__':
    app.run(debug=True)

from flask import Flask, render_template, jsonify
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen

app = Flask(__name__)

executor = ThreadPoolExecutor()

def run_command(command):
    process = Popen(command)
    process.wait()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_vehicle_data', methods=['POST'])
def generate_vehicle_data():
    for _ in range(3): # CAMBIAR PARA NUMERO DE ITERACIONES
        executor.submit(run_command, ['python3', 'generator_vehicle.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_viajes'])
    return jsonify({"status": "success", "message": "Generating Vehicles."})

@app.route('/generate_user_data', methods=['POST'])
def generate_user_data():
    for _ in range(3): # CAMBIAR PARA NUMERO DE ITERACIONES
        executor.submit(run_command, ['python3', 'generador_usuarios.py', '--project_id', 'dataproject-2-413010', '--topic_name', 'dp2_clientes'])
    return jsonify({"status": "success", "message": "Generating Users."})

if __name__ == '__main__':
    app.run(debug=False)


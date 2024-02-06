# EJECUTAR GENERADORES CON PYTHON

- generator.py

Para que funcione:

```powershell
python3 generator_vehicle.py `
    --project_id "dataproject-2-413010" `
    --topic_name "topic_dp2"
```
```powershell
python3 generador_usuarios.py `
    --project_id "dataproject-2-413010" `
    --topic_name "dp2_clientes"
```

De momento solo manda por coordenadas el viaje, me gustaría a poder ser que mandara todo de golpe. Este lee de la carpeta data los viajes.

- generatorbuck.py

La idea de este código es que lea las coordenadas de un bucket. Es igual que el anterior pero haciendo referencia a un bucket.

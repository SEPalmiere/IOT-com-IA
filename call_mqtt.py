import paho.mqtt.client as mqtt
import pandas as pd
import json
from datetime import datetime
import time
import os

def get_capture_time():
    while True:
        try:
            minutos = float(input("Digite o tempo total de captura em minutos: "))
            if minutos > 0:
                return minutos
            else:
                print("Por favor, digite um número maior que zero.")
        except ValueError:
            print("Por favor, digite um número válido.")

# Configurações do MQTT
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
TOPIC_TEMPERATURE = "sep/sensores/temperature"
TOPIC_HUMIDITY = "sep/sensores/humidity"
CSV_FILE = "sensor_data.csv"

# Variáveis globais para controle
current_data = {'timestamp': None, 'temperature': None, 'humidity': None}
program_end_time = None

def save_to_csv(new_record):
    if not os.path.exists(CSV_FILE):
        # Criar novo arquivo com cabeçalho
        df = pd.DataFrame([new_record])
        df.to_csv(CSV_FILE, index=False)
        print(f"\nNovo arquivo criado: {CSV_FILE}")
    else:
        # Anexar ao arquivo existente
        df = pd.DataFrame([new_record])
        df.to_csv(CSV_FILE, mode='a', header=False, index=False)
    
    print(f"Registro salvo: {new_record}")

def process_message(topic, value):
    global current_data
    
    # Atualizar timestamp
    current_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Atualizar valor correspondente
    if topic == TOPIC_TEMPERATURE:
        current_data['temperature'] = value
    elif topic == TOPIC_HUMIDITY:
        current_data['humidity'] = value
    
    # Verificar se temos ambos os valores
    if current_data['temperature'] is not None and current_data['humidity'] is not None:
        # Criar cópia dos dados atuais
        record_to_save = current_data.copy()
        # Resetar dados atuais
        current_data = {'timestamp': None, 'temperature': None, 'humidity': None}
        # Salvar registro completo
        save_to_csv(record_to_save)

def on_message(client, userdata, msg):
    global program_end_time
    
    # Verificar se é hora de encerrar o programa
    if program_end_time and time.time() >= program_end_time:
        client.disconnect()
        return
    
    topic = msg.topic
    payload = msg.payload.decode()
    
    try:
        # Tentar decodificar o JSON
        payload_dict = json.loads(payload)
        
        # Verificar diferentes formatos possíveis
        if isinstance(payload_dict, dict):
            value = payload_dict.get('value', payload_dict.get('data', None))
            if value is None and len(payload_dict) == 1:
                value = list(payload_dict.values())[0]
        else:
            value = payload_dict
        
        if value is not None:
            try:
                value = float(value)
                print(f"Dados recebidos - {topic}: {value}")
                process_message(topic, value)
            except ValueError:
                print(f"Valor inválido recebido: {value}")
            
    except (json.JSONDecodeError, ValueError) as e:
        try:
            value = float(payload)
            print(f"Dados recebidos - {topic}: {value}")
            process_message(topic, value)
        except ValueError:
            print(f"Erro ao processar payload: {payload}")

def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker com código de resultado: {rc}")
    client.subscribe([(TOPIC_TEMPERATURE, 0), (TOPIC_HUMIDITY, 0)])
    print(f"Inscrito nos tópicos: {TOPIC_TEMPERATURE} e {TOPIC_HUMIDITY}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Desconexão inesperada. Tentando reconectar...")
    else:
        print("Desconectado do broker")

def main():
    global program_end_time
    
    # Obter tempo de captura do usuário
    minutos = get_capture_time()
    program_end_time = time.time() + (minutos * 60)
    
    # Configuração do cliente MQTT
    client = mqtt.Client(clean_session=True)
    client.on_message = on_message
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Conectar ao broker
    print(f"Conectando ao broker {MQTT_BROKER}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Erro ao conectar ao broker: {str(e)}")
        return

    print(f"\nIniciando captura por {minutos:.1f} minutos...")
    if os.path.exists(CSV_FILE):
        print(f"Anexando dados ao arquivo existente: {CSV_FILE}")
    else:
        print(f"Criando novo arquivo: {CSV_FILE}")
    
    try:
        # Loop principal
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nCaptura interrompida pelo usuário")
    finally:
        client.disconnect()
        print("\nPrograma encerrado")

if __name__ == "__main__":
    main()
# Eventlet'i etkinleştir
import eventlet

eventlet.monkey_patch()

import json
import threading
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from datetime import datetime
import pika
import logging
from flask_cors import CORS
# Import the updated PostgreSQL database module functions
from db_module import init_db, kontrol_db, update_in_db, save_to_db, search_persons

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")


RABBITMQ_QUEUE = "deneme103"


def consume_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        def callback(ch, method, properties, body):
            try:
                data_str = body.decode("utf-8")
                data = json.loads(data_str)  # JSON'ı parse et


                if 'Entity_id' not in data or not data['Entity_id']:
                    data['Entity_id'] = f"unknown_{datetime.now().timestamp()}"

                logger.info(f"Alınan veri: {data.get('Name', 'Unknown')} - {data.get('Entity_id', 'Unknown')}")


                enrich_data(data)


                exists = kontrol_db(data)

                if exists:

                    update_in_db(data)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    message = {
                        "name": f"{data.get('Name', 'Unknown')} - {data.get('Entity_id', 'Unknown')}",
                        "data": data,
                        "timestamp": timestamp,
                        "alarm": "red"
                    }
                    logger.info(f"Veri güncellendi (ALARM): {data.get('Name', 'Unknown')}")
                else:

                    save_to_db(data)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    message = {
                        "name": f"{data.get('Name', 'Unknown')} - {data.get('Entity_id', 'Unknown')}",
                        "data": data,
                        "timestamp": timestamp,
                        "alarm": "normal"
                    }
                    logger.info(f"Yeni veri eklendi (NORMAL): {data.get('Name', 'Unknown')}")


                eventlet.spawn(send_via_websocket, message)
            except Exception as e:
                logger.error(f"Mesaj işleme hatası: {str(e)}")

                logger.error(traceback.format_exc())

        def send_via_websocket(message):

            try:
                with app.app_context():
                    socketio.emit("new_data", message)
                    logger.info(f"WebSocket üzerinden veri gönderildi: {message['name']}")
            except Exception as e:
                logger.error(f"WebSocket emit hatası: {str(e)}")

        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)
        logger.info("RabbitMQ dinleniyor...")
        channel.start_consuming()
    except Exception as e:
        logger.error(f"RabbitMQ bağlantı hatası: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def enrich_data(data):
    """Gelen verileri zenginleştir - eksik alanları doldur"""

    if 'Photo_url' not in data or not data['Photo_url']:

        if 'Thumbnail_url' in data and data['Thumbnail_url']:
            data['Photo_url'] = data['Thumbnail_url']

        elif data.get('Gender', '').lower() == 'f':
            data['Photo_url'] = 'https://www.interpol.int/bundles/interpolfront/images/fallback_picture_female.png'
        else:
            data['Photo_url'] = 'https://www.interpol.int/bundles/interpolfront/images/fallback_picture.png'


    if 'Gender' in data and not data.get('Sex', ''):
        data['Sex'] = 'Female' if data['Gender'].lower() == 'f' else 'Male'


    required_fields = ['Name', 'Sex', 'Nationalities', 'Age', 'Crime', 'Status']
    for field in required_fields:
        if field not in data or not data[field]:
            data[field] = 'Belirtilmemiş'

    if 'Age' in data and 'Date_of_birth' not in data:
        data['Date_of_birth'] = data['Age']


# Ana route
@app.route('/')
def index():
    return render_template('sayfa.html')


@app.route('/search', methods=['POST'])
def search():
    try:
        logger.info("Arama isteği alındı")
        data = request.json
        logger.info(f"İstek verileri: {data}")

        name = data.get('name', '')
        surname = data.get('surname', '')
        country = data.get('country', '')

        logger.info(f"Arama kriterleri: name={name}, surname={surname}, country={country}")

        results = search_persons(name, surname, country)
        logger.info(f"Arama sonuçları: {len(results)} kayıt bulundu")

        return jsonify({
            'success': True,
            'results': results
        })
    except Exception as e:
        logger.error(f"Arama hatası: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == "__main__":

    init_db()


    thread = threading.Thread(target=consume_rabbitmq, daemon=True)
    thread.start()


    socketio.run(app, debug=True, host="0.0.0.0", port=5000)
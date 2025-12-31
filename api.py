from flask import Flask, request, jsonify
import requests
import sqlite3
from datetime import datetime
import threading
import time
import os
import queue
import logging

# IMPORTANTE: Instale flask-cors primeiro!
# No console do Replit: pip install flask-cors
from flask_cors import CORS

app = Flask(__name__)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuração CORS
CORS(app)

# Middleware CORS
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# Webhooks
WEBHOOKS = {
    "NORMAL_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361732523327730/aCZn_oDnIDjOoHzCkrPk_x9ohfSFWSO9kNzkSFo0kYNxmZIyrOcrrqSN80S3tQs_LINk",
    "SPECIAL_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361536078905479/IptfKoKAO-imuZ39zysfeIBoHb-0ZIqOHkYHTc2AA7TqscwZA5xn8vKQmc4RbgJ5rZUP",
    "ULTRA_HIGH_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361629880582239/tpNHrWPlXGi8SyStifJ-A0mMYHLSIkP2kE_UzW6rZRRbS8xtLxmN1CvIk7081pbdo6eX",
    "BRAINROT_150M_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455430968575000729/4GH6iNeP3K6EeCtmFja1KzYxqGSICaXxtJURaZVq9LWzSsT9SwKGVw2ZqVUzMAqhFQpf"
}

# Banco de dados
DB_FILE = "servers.db"

# Fila para processamento assíncrono
discord_queue = queue.Queue()

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS sent_servers
                     (job_id TEXT PRIMARY KEY,
                      timestamp DATETIME,
                      webhook_type TEXT,
                      category TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS sent_brainrot_150m
                     (job_id TEXT PRIMARY KEY,
                      timestamp DATETIME)''')
        conn.commit()
        conn.close()
        logger.info("✅ Banco de dados inicializado")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar banco: {e}")

def send_to_discord(webhook_url, data):
    """Envia dados para webhook do Discord com timeout"""
    try:
        # Adicionar timeout para evitar bloqueio
        response = requests.post(
            webhook_url, 
            json=data, 
            timeout=10  # 10 segundos de timeout
        )
        if response.status_code == 204:
            logger.info(f"✅ Mensagem enviada para Discord: {response.status_code}")
            return True
        else:
            logger.warning(f"⚠️ Resposta inesperada do Discord: {response.status_code}")
            return False
    except requests.exceptions.Timeout:
        logger.error(f"⚠️ Timeout ao enviar para {webhook_url}")
        return False
    except Exception as e:
        logger.error(f"❌ Erro ao enviar para Discord: {e}")
        return False

def discord_worker():
    """Worker que processa mensagens do Discord em segundo plano"""
    while True:
        try:
            webhook_url, data = discord_queue.get()
            logger.info(f"📤 Processando mensagem na fila (tamanho: {discord_queue.qsize()})")
            send_to_discord(webhook_url, data)
            discord_queue.task_done()
        except Exception as e:
            logger.error(f"Erro no worker Discord: {e}")
            time.sleep(5)

def was_server_sent(job_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT job_id FROM sent_servers WHERE job_id = ?", (job_id,))
        result = c.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Erro ao verificar servidor: {e}")
        return False

def mark_server_sent(job_id, webhook_type, category):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO sent_servers VALUES (?, ?, ?, ?)",
                  (job_id, datetime.now(), webhook_type, category))
        conn.commit()
        conn.close()
        logger.info(f"📝 Servidor marcado como enviado: {job_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no banco: {e}")

def determine_webhook_type(data):
    """Determina qual webhook usar baseado nos dados"""
    category = data.get('category', '').upper()
    player_count = data.get('player_count', 0)
    place_name = str(data.get('place_name', '')).upper()
    
    # Brainrot 150M (prioridade máxima)
    if "150M" in place_name or "150 M" in place_name or "150.000.000" in place_name:
        return "BRAINROT_150M_WEBHOOK"
    
    # Ultra High (servidores grandes)
    elif player_count > 30 or category in ["ULTRA", "VIP", "EXCLUSIVE"]:
        return "ULTRA_HIGH_WEBHOOK"
    
    # Special (servidores especiais)
    elif category in ["SPECIAL", "EVENT", "HOLIDAY"]:
        return "SPECIAL_WEBHOOK"
    
    # Normal (todos os outros)
    else:
        return "NORMAL_WEBHOOK"

def prepare_brainrot_150m_embed(data, color):
    """Embed específica para Brainrot 150M+ (SEM Job ID)"""
    server_id = data.get('server_id', 'N/A')
    place_name = data.get('place_name', 'Unknown Place')
    player_count = data.get('player_count', 0)
    max_players = data.get('max_players', 0)
    category = data.get('category', 'UNKNOWN')
    
    embed = {
        "title": f"🔥 BRAINROT 150M+ DETECTADO!",
        "description": f"**🎮 {place_name}**\n🚨 Servidor com mais de 150M de visitas!",
        "color": color,
        "fields": [
            {
                "name": "👥 Jogadores",
                "value": f"{player_count}/{max_players}",
                "inline": True
            },
            {
                "name": "🏷️ Categoria",
                "value": category,
                "inline": True
            },
            {
                "name": "🆔 Server ID",
                "value": f"`{server_id}`",
                "inline": False
            }
        ],
        "thumbnail": {
            "url": "https://tr.rbxcdn.com/9f89e27f1fbdcd38ae9259145436f6b7/420/420/Image/Png"
        },
        "timestamp": datetime.now().isoformat(),
        "footer": {
            "text": "BrainRot Scanner • v1.0 • 150M+ ALERT"
        }
    }
    
    return {
        "embeds": [embed],
        "username": "BRAINROT 150M+ ALERT",
        "avatar_url": "https://tr.rbxcdn.com/9f89e27f1fbdcd38ae9259145436f6b7/420/420/Image/Png"
    }

def prepare_normal_embed(data, webhook_type, color):
    """Embed para outros webhooks (COM Job ID)"""
    server_id = data.get('server_id', 'N/A')
    place_name = data.get('place_name', 'Unknown Place')
    player_count = data.get('player_count', 0)
    max_players = data.get('max_players', 0)
    category = data.get('category', 'UNKNOWN')
    job_id = data.get('job_id', 'N/A')
    
    # Determinar título baseado no tipo
    title_map = {
        "ULTRA_HIGH_WEBHOOK": "🚀 Servidor Ultra High",
        "SPECIAL_WEBHOOK": "⭐ Servidor Especial",
        "NORMAL_WEBHOOK": "📡 Novo Servidor"
    }
    
    embed = {
        "title": title_map.get(webhook_type, "📡 Novo Servidor"),
        "description": f"**{place_name}**",
        "color": color,
        "fields": [
            {
                "name": "👥 Jogadores",
                "value": f"{player_count}/{max_players}",
                "inline": True
            },
            {
                "name": "🏷️ Categoria",
                "value": category,
                "inline": True
            },
            {
                "name": "🆔 Server ID",
                "value": f"`{server_id}`",
                "inline": False
            },
            {
                "name": "📊 Job ID",
                "value": f"`{job_id}`",
                "inline": False
            }
        ],
        "timestamp": datetime.now().isoformat(),
        "footer": {
            "text": f"BrainRot Scanner • {webhook_type.replace('_WEBHOOK', '')}"
        }
    }
    
    return {
        "embeds": [embed],
        "username": "BrainRot Scanner",
        "avatar_url": "https://tr.rbxcdn.com/9f89e27f1fbdcd38ae9259145436f6b7/420/420/Image/Png"
    }

def prepare_discord_embed(data, webhook_type):
    """Prepara embed para Discord"""
    
    # Determinar cor do embed
    color_map = {
        "BRAINROT_150M_WEBHOOK": 0xFF0000,  # Vermelho
        "ULTRA_HIGH_WEBHOOK": 0xFFFF00,     # Amarelo
        "SPECIAL_WEBHOOK": 0x00FF00,        # Verde
        "NORMAL_WEBHOOK": 0x0099FF          # Azul
    }
    
    # Lógica específica para Brainrot 150M
    if webhook_type == "BRAINROT_150M_WEBHOOK":
        return prepare_brainrot_150m_embed(data, color_map[webhook_type])
    
    # Lógica para outros webhooks (com Job ID)
    return prepare_normal_embed(data, webhook_type, color_map.get(webhook_type, 0x0099FF))

# ROTA RAIZ - ADICIONE ESTA ROTA!
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "api": "brainrot-scanner",
        "version": "2.0",
        "endpoints": {
            "GET": ["/", "/health", "/servers", "/test-webhook/<type>", "/queue-status"],
            "POST": ["/webhook-filter", "/test"]
        },
        "replit_url": "https://infinity--p808409.replit.app",
        "timestamp": datetime.now().isoformat(),
        "queue_size": discord_queue.qsize()
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "queue_workers": threading.active_count() - 1,
        "queue_size": discord_queue.qsize()
    })

@app.route('/queue-status', methods=['GET'])
def queue_status():
    return jsonify({
        "queue_size": discord_queue.qsize(),
        "queue_unfinished_tasks": discord_queue.unfinished_tasks,
        "active_threads": threading.active_count(),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/webhook-filter', methods=['POST', 'OPTIONS'])
def webhook_filter():
    # CORS preflight
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    
    try:
        logger.info(f"\n{'='*50}")
        logger.info("📥 Nova requisição recebida")
        
        # Verificar se tem dados
        if not request.data:
            return jsonify({"status": "error", "message": "No data"}), 400
        
        # Tentar parsear JSON
        data = {}
        try:
            data = request.get_json()
            if not data:
                import json
                data = json.loads(request.data)
        except Exception as e:
            logger.error(f"❌ Erro no JSON: {e}")
            return jsonify({"status": "error", "message": f"Invalid JSON: {str(e)}"}), 400
        
        logger.info(f"📋 Dados recebidos - Job ID: {data.get('job_id', 'N/A')}")
        
        # Validar job_id
        job_id = data.get('job_id')
        if not job_id:
            return jsonify({"status": "error", "message": "Missing job_id"}), 400
        
        # Verificar duplicata
        if was_server_sent(job_id):
            logger.info(f"📭 Duplicata ignorada: {job_id}")
            return jsonify({"status": "duplicate", "message": "Already sent"}), 200
        
        # Determinar webhook correto baseado nos dados
        webhook_type = determine_webhook_type(data)
        webhook_url = WEBHOOKS.get(webhook_type, WEBHOOKS["NORMAL_WEBHOOK"])
        
        # Log específico para Brainrot 150M
        if webhook_type == "BRAINROT_150M_WEBHOOK":
            logger.info(f"🔥🔥🔥 ALERTA BRAINROT 150M+ DETECTADO! 🔥🔥🔥")
            logger.info(f"📛 Place: {data.get('place_name', 'Unknown')}")
            logger.info(f"👥 Players: {data.get('player_count', 0)}")
            logger.info(f"🆔 Server ID: {data.get('server_id', 'N/A')}")
        
        # Preparar embed para Discord
        discord_data = prepare_discord_embed(data, webhook_type)
        
        # Enviar para Discord (assíncrono)
        discord_queue.put((webhook_url, discord_data))
        
        # Marcar como enviado
        category = data.get('category', 'UNKNOWN')
        mark_server_sent(job_id, webhook_type, category)
        
        logger.info(f"✅ Enfileirado para Discord: {job_id} -> {webhook_type}")
        logger.info(f"📊 Fila atual: {discord_queue.qsize()} mensagens")
        logger.info(f"{'='*50}\n")
        
        return jsonify({
            "status": "success",
            "message": "Received and queued for Discord",
            "job_id": job_id,
            "webhook_type": webhook_type,
            "queue_position": discord_queue.qsize(),
            "server_id": data.get('server_id', 'unknown'),
            "test_mode": False
        }), 200
        
    except Exception as e:
        logger.error(f"🔥 ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/servers', methods=['GET'])
def list_servers():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT job_id, timestamp, webhook_type, category FROM sent_servers ORDER BY timestamp DESC LIMIT 20")
        servers = c.fetchall()
        conn.close()
        
        server_list = []
        for server in servers:
            server_list.append({
                "job_id": server[0],
                "timestamp": server[1],
                "webhook_type": server[2],
                "category": server[3]
            })
        
        return jsonify({
            "status": "success",
            "count": len(server_list),
            "servers": server_list,
            "total_in_queue": discord_queue.qsize()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/test', methods=['GET', 'POST'])
def test():
    """Endpoint de teste"""
    if request.method == 'GET':
        return jsonify({
            "status": "test",
            "message": "Test endpoint works!",
            "use_post": "Send POST with JSON data",
            "timestamp": datetime.now().isoformat()
        })
    
    # POST
    try:
        data = request.get_json() or {}
        
        # Simular processamento
        job_id = data.get('job_id', f"test_{int(time.time())}")
        webhook_type = determine_webhook_type(data)
        
        return jsonify({
            "status": "success",
            "message": "Test data received",
            "your_data": data,
            "determined_webhook": webhook_type,
            "test_job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "note": "This was a test - no Discord message sent"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/test-webhook/<webhook_type>', methods=['GET'])
def test_webhook(webhook_type):
    """Testar conexão com webhook do Discord"""
    if webhook_type not in WEBHOOKS:
        return jsonify({"status": "error", "message": "Webhook type not found"}), 404
    
    test_data = {
        "embeds": [{
            "title": "🧪 Teste de Webhook",
            "description": f"Testando {webhook_type}",
            "color": 0x00FF00,
            "timestamp": datetime.now().isoformat(),
            "fields": [
                {
                    "name": "Status",
                    "value": "✅ Funcionando corretamente",
                    "inline": True
                },
                {
                    "name": "Hora",
                    "value": datetime.now().strftime("%H:%M:%S"),
                    "inline": True
                }
            ]
        }]
    }
    
    try:
        response = requests.post(WEBHOOKS[webhook_type], json=test_data, timeout=10)
        return jsonify({
            "status": "success",
            "webhook": webhook_type,
            "discord_status": response.status_code,
            "message": "Test sent successfully",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "webhook": webhook_type,
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    # Inicializar banco
    init_db()
    
    # Iniciar worker do Discord em thread separada
    discord_worker_thread = threading.Thread(target=discord_worker, daemon=True)
    discord_worker_thread.start()
    
    # Configurar porta
    port = int(os.environ.get("PORT", 5000))
    
    print("\n" + "="*60)
    print("🚀 BRAINROT SCANNER API - REINICIADA (v2.0)")
    print("="*60)
    print(f"📡 Porta: {port}")
    print(f"🌐 URL: https://infinity--p808409.replit.app")
    print("🔗 Endpoints disponíveis:")
    print("   GET  /                  - Status da API")
    print("   GET  /health            - Health check")
    print("   GET  /servers           - Lista de servidores")
    print("   GET  /queue-status      - Status da fila")
    print("   GET  /test              - Teste GET")
    print("   POST /test              - Teste POST")
    print("   GET  /test-webhook/<type> - Testar webhook Discord")
    print("   POST /webhook-filter    - Receber dados do Roblox")
    print("="*60)
    print("🎯 Funcionalidades:")
    print("   • Brainrot 150M+ (SEM Job ID na embed)")
    print("   • Processamento assíncrono")
    print("   • Prevenção de duplicatas")
    print("   • Logs detalhados")
    print("="*60)
    print("✅ API PRONTA!")
    print("="*60 + "\n")
    
    logger.info("API inicializada com sucesso")
    logger.info(f"Worker Discord iniciado: {discord_worker_thread.is_alive()}")
    
    # Iniciar servidor
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

from flask import Flask, request, jsonify
import requests
import sqlite3
from datetime import datetime
import threading
import time
import queue
import logging

app = Flask(__name__)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('brainrot_api.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuração dos webhooks
WEBHOOKS = {
    "NORMAL_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361732523327730/aCZn_oDnIDjOoHzCkrPk_x9ohfSFWSO9kNzkSFo0kYNxmZIyrOcrrqSN80S3tQs_LINk",
    "SPECIAL_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361536078905479/IptfKoKAO-imuZ39zysfeIBoHb-0ZIqOHkYHTc2AA7TqscwZA5xn8vKQmc4RbgJ5rZUP",
    "ULTRA_HIGH_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455361629880582239/tpNHrWPlXGi8SyStifJ-A0mMYHLSIkP2kE_UzW6rZRRbS8xtLxmN1CvIk7081pbdo6eX",
    "BRAINROT_150M_WEBHOOK": "https://ptb.discord.com/api/webhooks/1455430968575000729/4GH6iNeP3K6EeCtmFja1KzYxqGSICaXxtJURaZVq9LWzSsT9SwKGVw2ZqVUzMAqhFQpf"
}

# Configuração do banco de dados
DB_FILE = "servers.db"

# Fila para processamento assíncrono
discord_queue = queue.Queue()

def init_db():
    """Inicializa o banco de dados"""
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

def was_server_sent(job_id):
    """Verifica se o servidor já foi enviado"""
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

def was_brainrot_150m_sent(job_id):
    """Verifica se o alerta brainrot 150M já foi enviado"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT job_id FROM sent_brainrot_150m WHERE job_id = ?", (job_id,))
        result = c.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Erro ao verificar brainrot 150M: {e}")
        return False

def mark_server_sent(job_id, webhook_type, category):
    """Marca o servidor como enviado"""
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

def mark_brainrot_150m_sent(job_id):
    """Marca o alerta brainrot 150M como enviado"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO sent_brainrot_150m VALUES (?, ?)",
                  (job_id, datetime.now()))
        conn.commit()
        conn.close()
        logger.info(f"📝 Brainrot 150M marcado como enviado: {job_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar brainrot 150M: {e}")

def send_to_discord_webhook(payload_data, webhook_type, is_brainrot_150m=False):
    """Envia embed para o Discord"""
    if webhook_type not in WEBHOOKS:
        logger.error(f"Webhook type não encontrado: {webhook_type}")
        return False
    
    webhook_url = WEBHOOKS[webhook_type]
    
    try:
        response = requests.post(webhook_url, json=payload_data, timeout=10)
        if response.status_code == 204:
            if is_brainrot_150m:
                logger.info(f"🚨 Alerta Brainrot 150M+ enviado com sucesso!")
            else:
                logger.info(f"✅ Embed enviado para Discord com sucesso!")
            return True
        else:
            logger.warning(f"⚠️ Resposta inesperada do Discord: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.Timeout:
        logger.error(f"⚠️ Timeout ao enviar para {webhook_type}")
        return False
    except Exception as e:
        logger.error(f"❌ Erro ao enviar para Discord: {e}")
        return False

def check_brainrot_150m(embed_data):
    """Verifica se há brainrot > 150M e prepara alerta"""
    job_id = embed_data.get('job_id', 'unknown')
    embed_info = embed_data.get('embed_info', {})
    top_brainrots = embed_info.get('top_brainrots', [])
    
    if was_brainrot_150m_sent(job_id):
        logger.info(f"📭 Brainrot 150M+ duplicado ignorado: {job_id}")
        return None
    
    has_high_brainrot = False
    highest_brainrot = None
    
    for brainrot in top_brainrots:
        numeric_gen = brainrot.get('numericGen', 0)
        if numeric_gen >= 150000000:
            has_high_brainrot = True
            if not highest_brainrot or numeric_gen > highest_brainrot.get('numericGen', 0):
                highest_brainrot = brainrot
    
    if not has_high_brainrot:
        return None
    
    # Construir embed para brainrot 150M (SEM Job ID)
    description = "🚨 **Brainrot 150M+ DETECTADO!** 🚨\n\n"
    for i, brainrot in enumerate(top_brainrots[:5], 1):  # Limitar aos 5 primeiros
        numeric_gen = brainrot.get('numericGen', 0)
        if numeric_gen >= 150000000:
            name = brainrot.get('name', 'Unknown')
            value_per_second = brainrot.get('valuePerSecond', '0/s')
            description += f"**{i}º** - {name}: **{value_per_second}**\n"
    
    # Formatar Server ID (sem Job ID)
    server_id = embed_data.get('server_id', 'N/A')
    
    embed = {
        "title": f"👑 {highest_brainrot.get('name', 'Unknown')}",
        "description": description,
        "color": 16711680,  # Vermelho
        "fields": [
            {
                "name": "👥 Jogadores no Servidor",
                "value": f"**{embed_data.get('players', 0)}/{embed_data.get('max_players', 0)}**",
                "inline": True
            },
            {
                "name": "📊 Maior Geração",
                "value": f"**{highest_brainrot.get('valuePerSecond', '0/s')}**",
                "inline": True
            }
        ],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "thumbnail": {
            "url": "https://tr.rbxcdn.com/9f89e27f1fbdcd38ae9259145436f6b7/420/420/Image/Png"
        },
        "footer": {
            "text": "ALERTA BRAINROT 150M+ • Scanner Automático"
        }
    }
    
    return {
        "embeds": [embed],
        "username": "BRAINROT 150M+ ALERT"
    }

def prepare_normal_embed(data, webhook_type):
    """Prepara embed normal para Discord (COM Job ID)"""
    embed_info = data.get('embed_info', {})
    category_info = {
        "ULTRA_HIGH": {"color": 10181046, "emoji": "💎", "name": "ULTRA HIGH"},
        "SPECIAL": {"color": 16766720, "emoji": "🔥", "name": "ESPECIAL"},
        "NORMAL": {"color": 5793266, "emoji": "⭐", "name": "NORMAL"}
    }
    
    category = data.get('category', 'NORMAL')
    info = category_info.get(category, category_info["NORMAL"])
    
    # Formatar Server ID e Job ID
    server_id = data.get('server_id', 'N/A')
    job_id = data.get('job_id', 'Unknown')
    
    # Construir embed
    embed = {
        "title": f"{info['emoji']} {embed_info.get('highest_brainrot', {}).get('name', 'Unknown')}",
        "description": embed_info.get('description', ''),
        "color": info['color'],
        "fields": [
            {
                "name": "🌐 Informações do Servidor",
                "value": f"**Jogadores:** {data.get('players', 0)}/{data.get('max_players', 0)}\n"
                        f"**Server ID:** ```{server_id}```\n"
                        f"**Job ID:** ```{job_id}```\n"
                        f"**Total encontrados:** {data.get('total_found', 0)}",
                "inline": False
            }
        ],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "footer": {
            "text": f"Scanner Automático • {info['name']}"
        }
    }
    
    return {
        "embeds": [embed],
        "username": "BrainRot Scanner",
        "avatar_url": "https://tr.rbxcdn.com/9f89e27f1fbdcd38ae9259145436f6b7/420/420/Image/Png"
    }

def determine_webhook_type(data):
    """Determina qual webhook usar baseado nos dados"""
    category = data.get('category', '').upper()
    player_count = data.get('player_count', 0)
    embed_info = data.get('embed_info', {})
    top_brainrots = embed_info.get('top_brainrots', [])
    
    # Primeiro verificar Brainrot 150M
    for brainrot in top_brainrots:
        if brainrot.get('numericGen', 0) >= 150000000:
            return "BRAINROT_150M_WEBHOOK"
    
    # Depois outras categorias
    if player_count > 30 or category in ["ULTRA_HIGH", "ULTRA", "VIP", "EXCLUSIVE"]:
        return "ULTRA_HIGH_WEBHOOK"
    elif category in ["SPECIAL", "EVENT", "HOLIDAY"]:
        return "SPECIAL_WEBHOOK"
    else:
        return "NORMAL_WEBHOOK"

def discord_worker():
    """Worker que processa mensagens do Discord em segundo plano"""
    while True:
        try:
            webhook_url, payload_data, is_brainrot_150m, job_id = discord_queue.get()
            logger.info(f"📤 Processando mensagem na fila (tamanho: {discord_queue.qsize()})")
            
            success = send_to_discord_webhook(payload_data, webhook_url, is_brainrot_150m)
            
            if success and is_brainrot_150m:
                mark_brainrot_150m_sent(job_id)
                logger.info(f"🚨 Alerta brainrot 150M+ processado: {job_id}")
            
            discord_queue.task_done()
        except Exception as e:
            logger.error(f"Erro no worker Discord: {e}")
            time.sleep(5)

@app.route('/webhook-filter', methods=['POST'])
def webhook_filter():
    """Endpoint principal para filtrar e encaminhar webhooks"""
    try:
        data = request.json
        
        if not data:
            logger.warning("Requisição sem dados recebida")
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        job_id = data.get('job_id')
        if not job_id:
            logger.warning("Requisição sem job_id recebida")
            return jsonify({"status": "error", "message": "No job_id provided"}), 400
        
        # Log para debug
        logger.info(f"\n{'='*50}")
        logger.info(f"📥 Recebido request do Roblox")
        logger.info(f"📋 Job ID: {job_id}")
        logger.info(f"🔤 Server ID: {repr(data.get('server_id', 'N/A'))}")
        logger.info(f"📊 Dados recebidos - Players: {data.get('players', 0)}/{data.get('max_players', 0)}")
        
        # Verificar se o servidor já foi enviado
        if was_server_sent(job_id):
            logger.info(f"📭 Servidor duplicado ignorado: {job_id}")
            return jsonify({"status": "duplicate", "message": "Server already sent"}), 200
        
        # Determinar webhook type automaticamente
        webhook_type = determine_webhook_type(data)
        logger.info(f"🎯 Webhook type determinado: {webhook_type}")
        
        # Verificar brainrot 150M e preparar embed
        brainrot_embed = check_brainrot_150m(data)
        
        # Preparar payloads para a fila
        if brainrot_embed and webhook_type == "BRAINROT_150M_WEBHOOK":
            # Enfileirar alerta brainrot 150M
            discord_queue.put(("BRAINROT_150M_WEBHOOK", brainrot_embed, True, job_id))
            logger.info(f"🔥 Alerta Brainrot 150M+ enfileirado: {job_id}")
        
        # Sempre enfileirar embed normal (se não for brainrot 150M)
        if webhook_type != "BRAINROT_150M_WEBHOOK":
            normal_embed = prepare_normal_embed(data, webhook_type)
            discord_queue.put((webhook_type, normal_embed, False, job_id))
            logger.info(f"📡 Embed normal enfileirado: {job_id} -> {webhook_type}")
        
        # Marcar como enviado imediatamente (para evitar duplicatas)
        category = data.get('category', 'UNKNOWN')
        mark_server_sent(job_id, webhook_type, category)
        
        logger.info(f"✅ Request processado com sucesso")
        logger.info(f"📊 Fila atual: {discord_queue.qsize()} mensagens")
        logger.info(f"{'='*50}")
        
        return jsonify({
            "status": "success",
            "message": "Received and queued for processing",
            "job_id": job_id,
            "webhook_type": webhook_type,
            "queue_position": discord_queue.qsize(),
            "server_id": data.get('server_id', 'unknown'),
            "processing_mode": "async"
        }), 200
            
    except Exception as e:
        logger.error(f"❌ Erro no servidor Python: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de verificação de saúde"""
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "queue_size": discord_queue.qsize(),
        "active_threads": threading.active_count()
    }), 200

@app.route('/servers', methods=['GET'])
def list_servers():
    """Lista todos os servidores enviados"""
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
            "servers": server_list, 
            "count": len(server_list),
            "queue_size": discord_queue.qsize()
        }), 200
    except Exception as e:
        logger.error(f"Erro ao listar servidores: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/queue-status', methods=['GET'])
def queue_status():
    """Status da fila de processamento"""
    return jsonify({
        "queue_size": discord_queue.qsize(),
        "queue_unfinished_tasks": discord_queue.unfinished_tasks,
        "active_threads": threading.active_count(),
        "timestamp": datetime.now().isoformat()
    })

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

def cleanup_old_entries():
    """Limpa entradas antigas do banco de dados"""
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            # Remove entradas com mais de 24 horas
            c.execute("DELETE FROM sent_servers WHERE timestamp < datetime('now', '-24 hours')")
            c.execute("DELETE FROM sent_brainrot_150m WHERE timestamp < datetime('now', '-24 hours')")
            deleted = conn.total_changes
            conn.commit()
            conn.close()
            if deleted > 0:
                logger.info(f"🧹 Banco de dados limpo: {deleted} entradas removidas")
        except Exception as e:
            logger.error(f"Erro ao limpar banco: {e}")
        
        # Executa a cada hora
        time.sleep(3600)

if __name__ == '__main__':
    # Inicializar banco de dados
    init_db()
    
    # Iniciar worker do Discord em thread separada
    discord_worker_thread = threading.Thread(target=discord_worker, daemon=True)
    discord_worker_thread.start()
    
    # Iniciar thread de limpeza
    cleanup_thread = threading.Thread(target=cleanup_old_entries, daemon=True)
    cleanup_thread.start()
    
    logger.info("\n" + "="*60)
    logger.info("🚀 BRAINROT SCANNER API - INICIADA")
    logger.info("="*60)
    logger.info("🔗 Endpoints disponíveis:")
    logger.info("   POST /webhook-filter    - Receber dados do Roblox")
    logger.info("   GET  /health            - Health check")
    logger.info("   GET  /servers           - Lista de servidores")
    logger.info("   GET  /queue-status      - Status da fila")
    logger.info("   GET  /test-webhook/<type> - Testar webhook Discord")
    logger.info("="*60)
    logger.info("🎯 Funcionalidades:")
    logger.info("   • Brainrot 150M+ (SEM Job ID na embed)")
    logger.info("   • Processamento assíncrono")
    logger.info("   • Prevenção de duplicatas")
    logger.info("   • Detecção automática de categoria")
    logger.info("="*60)
    logger.info("✅ API PRONTA!")
    logger.info("="*60)
    
    # Iniciar servidor Flask
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

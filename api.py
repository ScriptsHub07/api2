from flask import Flask, request, jsonify
import requests
import sqlite3
from datetime import datetime
import threading
import time
import concurrent.futures

app = Flask(__name__)

# Configuração dos webhooks
WEBHOOKS = {
    "NORMAL_WEBHOOK": "https://discord.com/api/webhooks/1450539120274313449/rLk9My7PAHiC26tjJ-jr05BjpeLGj8h1cCBLMx2KYybtuVAPGOAOa6Rc4AVn4w4_rjbg",
    "SPECIAL_WEBHOOK": "https://discord.com/api/webhooks/1450539120274313449/rLk9My7PAHiC26tjJ-jr05BjpeLGj8h1cCBLMx2KYybtuVAPGOAOa6Rc4AVn4w4_rjbg",
    "ULTRA_HIGH_WEBHOOK": "https://discord.com/api/webhooks/1450539120274313449/rLk9My7PAHiC26tjJ-jr05BjpeLGj8h1cCBLMx2KYybtuVAPGOAOa6Rc4AVn4w4_rjbg",
    "BRAINROT_150M_WEBHOOK": "https://discord.com/api/webhooks/1450539120274313449/rLk9My7PAHiC26tjJ-jr05BjpeLGj8h1cCBLMx2KYybtuVAPGOAOa6Rc4AVn4w4_rjbg"
}

# Configuração do banco de dados
DB_FILE = "servers.db"

# Pool de threads para envios paralelos
executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

# Cache em memória para verificações rápidas
server_cache = {}
cache_lock = threading.Lock()

def init_db():
    """Inicializa o banco de dados"""
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
    
    # Carregar cache
    load_cache()

def load_cache():
    """Carrega cache do banco de dados"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT job_id FROM sent_servers WHERE timestamp > datetime('now', '-6 hours')")
        servers = c.fetchall()
        c.execute("SELECT job_id FROM sent_brainrot_150m WHERE timestamp > datetime('now', '-6 hours')")
        brainrots = c.fetchall()
        conn.close()
        
        with cache_lock:
            server_cache.clear()
            for job_id in servers:
                server_cache[job_id[0]] = True
            for job_id in brainrots:
                server_cache[f"brainrot_{job_id[0]}"] = True
        
        print(f"📦 Cache carregado: {len(server_cache)} entradas")
    except Exception as e:
        print(f"❌ Erro ao carregar cache: {e}")

def was_server_sent_fast(job_id):
    """Verificação rápida usando cache"""
    with cache_lock:
        return job_id in server_cache

def was_brainrot_150m_sent_fast(job_id):
    """Verificação rápida usando cache"""
    with cache_lock:
        return f"brainrot_{job_id}" in server_cache

def mark_server_sent_fast(job_id, webhook_type, category):
    """Marca no cache e depois no banco em segundo plano"""
    # Adicionar ao cache primeiro (rápido)
    with cache_lock:
        server_cache[job_id] = True
    
    # Salvar no banco em segundo plano
    executor.submit(mark_server_sent_db, job_id, webhook_type, category)

def mark_brainrot_150m_sent_fast(job_id):
    """Marca no cache e depois no banco em segundo plano"""
    # Adicionar ao cache primeiro (rápido)
    with cache_lock:
        server_cache[f"brainrot_{job_id}"] = True
    
    # Salvar no banco em segundo plano
    executor.submit(mark_brainrot_150m_sent_db, job_id)

def mark_server_sent_db(job_id, webhook_type, category):
    """Marca no banco de dados (executado em segundo plano)"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO sent_servers VALUES (?, ?, ?, ?)",
                  (job_id, datetime.now(), webhook_type, category))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao salvar no banco: {e}")

def mark_brainrot_150m_sent_db(job_id):
    """Marca no banco de dados (executado em segundo plano)"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO sent_brainrot_150m VALUES (?, ?)",
                  (job_id, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao salvar brainrot 150M: {e}")

def send_to_discord_webhook_async(embed_data, webhook_type):
    """Envia embed para o Discord de forma assíncrona"""
    return executor.submit(send_to_discord_webhook_sync, embed_data, webhook_type)

def send_to_discord_webhook_sync(embed_data, webhook_type):
    """Função síncrona para enviar ao Discord"""
    if webhook_type not in WEBHOOKS:
        print(f"❌ Webhook type não encontrado: {webhook_type}")
        return False
    
    webhook_url = WEBHOOKS[webhook_type]
    
    # Construir embed rapidamente
    embed_info = embed_data.get('embed_info', {})
    category_info = {
        "ULTRA_HIGH": {"color": 10181046, "emoji": "💎", "name": "ULTRA HIGH"},
        "SPECIAL": {"color": 16766720, "emoji": "🔥", "name": "ESPECIAL"},
        "NORMAL": {"color": 5793266, "emoji": "⭐", "name": "NORMAL"}
    }
    
    category = embed_data.get('category', 'NORMAL')
    info = category_info.get(category, category_info["NORMAL"])
    
    # Formatar server_id rapidamente
    server_id = embed_data.get('server_id', 'N/A')
    if not server_id.startswith("```"):
        server_id = f"```{server_id}```"
    
    # Embed otimizado
    embed = {
        "title": f"{info['emoji']} {embed_info.get('highest_brainrot', {}).get('name', 'Unknown')}",
        "description": embed_info.get('description', '')[:200],  # Limitar tamanho
        "color": info['color'],
        "fields": [
            {
                "name": "🌐 Informações",
                "value": f"**Jogadores:** {embed_data['players']}/{embed_data['max_players']}\n"
                        f"**Server ID:** {server_id}\n"
                        f"**Encontrados:** {embed_data['total_found']}",
                "inline": False
            }
        ],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "footer": {"text": f"Scanner • {info['name']}"}
    }
    
    payload = {"embeds": [embed]}
    
    try:
        # Timeout mais curto para resposta rápida
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.status_code == 204:
            print(f"✅ Discord OK: {webhook_type}")
            return True
        else:
            print(f"⚠️ Discord {response.status_code}: {webhook_type}")
            return False
    except Exception as e:
        print(f"❌ Discord erro: {webhook_type} - {str(e)[:50]}")
        return False

def check_and_send_brainrot_150m_async(embed_data):
    """Verifica e envia brainrot 150M de forma assíncrona"""
    return executor.submit(check_and_send_brainrot_150m_sync, embed_data)

def check_and_send_brainrot_150m_sync(embed_data):
    """Função síncrona para verificar e enviar brainrot 150M"""
    job_id = embed_data['job_id']
    
    # Verificação rápida no cache
    if was_brainrot_150m_sent_fast(job_id):
        return False
    
    top_brainrots = embed_data.get('embed_info', {}).get('top_brainrots', [])
    
    has_high_brainrot = False
    highest_brainrot = None
    
    for brainrot in top_brainrots:
        if brainrot.get('numericGen', 0) >= 150000000:
            has_high_brainrot = True
            if not highest_brainrot or brainrot.get('numericGen', 0) > highest_brainrot.get('numericGen', 0):
                highest_brainrot = brainrot
    
    if not has_high_brainrot:
        return False
    
    # Embed brainrot 150M (SEM Job ID)
    description = "🚨 **Brainrot 150M+ DETECTADO!** 🚨\n\n"
    for i, brainrot in enumerate(top_brainrots[:3], 1):  # Limitar a 3
        if brainrot.get('numericGen', 0) >= 150000000:
            description += f"**{i}º** - {brainrot.get('name', 'Unknown')}: **{brainrot.get('valuePerSecond', '0/s')}**\n"
    
    embed = {
        "title": f"👑 {highest_brainrot.get('name', 'Unknown')}",
        "description": description,
        "color": 16711680,
        "fields": [
            {
                "name": "👥 Jogadores",
                "value": f"**{embed_data['players']}/{embed_data['max_players']}**",
                "inline": True
            },
            {
                "name": "📊 Maior Geração",
                "value": f"**{highest_brainrot.get('valuePerSecond', '0/s')}**",
                "inline": True
            }
        ],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "footer": {"text": "ALERTA BRAINROT 150M+"}
    }
    
    payload = {"embeds": [embed]}
    
    try:
        response = requests.post(WEBHOOKS["BRAINROT_150M_WEBHOOK"], json=payload, timeout=5)
        if response.status_code == 204:
            mark_brainrot_150m_sent_fast(job_id)
            print(f"🚨 Brainrot 150M+ enviado: {job_id}")
            return True
    except Exception as e:
        print(f"❌ Erro brainrot 150M: {str(e)[:50]}")
    
    return False

@app.route('/webhook-filter', methods=['POST'])
def webhook_filter():
    """Endpoint principal otimizado para velocidade"""
    start_time = time.time()
    
    try:
        data = request.json
        
        if not data:
            return jsonify({"status": "error", "message": "No data"}), 400
        
        job_id = data.get('job_id')
        if not job_id:
            return jsonify({"status": "error", "message": "No job_id"}), 400
        
        # LOG RÁPIDO
        print(f"\n⚡ Recebido: {job_id}")
        
        # Verificação RÁPIDA no cache
        if was_server_sent_fast(job_id):
            print(f"📭 Cache hit: {job_id}")
            return jsonify({"status": "duplicate", "message": "Already sent"}), 200
        
        webhook_type = data.get('webhook_type', 'NORMAL_WEBHOOK')
        
        # PROCESSAMENTO PARALELO
        futures = []
        
        # 1. Verificar brainrot 150M (paralelo)
        futures.append(check_and_send_brainrot_150m_async(data))
        
        # 2. Enviar para webhook normal (paralelo)
        if webhook_type in WEBHOOKS:
            futures.append(send_to_discord_webhook_async(data, webhook_type))
        
        # 3. Marcar como enviado (no cache primeiro)
        category = data.get('category', 'UNKNOWN')
        mark_server_sent_fast(job_id, webhook_type, category)
        
        # Responder IMEDIATAMENTE sem esperar pelos resultados
        processing_time = (time.time() - start_time) * 1000
        print(f"✅ Processado em {processing_time:.1f}ms")
        
        return jsonify({
            "status": "queued",
            "message": "Processing started",
            "job_id": job_id,
            "processing_time_ms": processing_time,
            "parallel_tasks": len(futures)
        }), 200
            
    except Exception as e:
        error_time = (time.time() - start_time) * 1000
        print(f"❌ Erro em {error_time:.1f}ms: {str(e)[:100]}")
        return jsonify({"status": "error", "message": "Server error"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check otimizado"""
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(server_cache),
        "thread_pool": executor._max_workers
    }), 200

@app.route('/servers', methods=['GET'])
def list_servers():
    """Lista servidores (otimizado)"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT job_id, timestamp, webhook_type FROM sent_servers ORDER BY timestamp DESC LIMIT 10")
        servers = c.fetchall()
        conn.close()
        
        server_list = []
        for server in servers:
            server_list.append({
                "job_id": server[0],
                "timestamp": server[1],
                "webhook_type": server[2]
            })
        
        return jsonify({
            "status": "success", 
            "servers": server_list, 
            "count": len(server_list),
            "cache_hits": len(server_cache)
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def background_cleanup():
    """Limpeza em background"""
    while True:
        try:
            # Limpar cache antigo
            with cache_lock:
                # Manter apenas as últimas 1000 entradas no cache
                if len(server_cache) > 1000:
                    # Remover entradas antigas (simplificado)
                    keys_to_remove = list(server_cache.keys())[:len(server_cache) - 800]
                    for key in keys_to_remove:
                        del server_cache[key]
                    print(f"🧹 Cache limpo: {len(keys_to_remove)} entradas")
            
            # Limpar banco (menos frequente)
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("DELETE FROM sent_servers WHERE timestamp < datetime('now', '-12 hours')")
            c.execute("DELETE FROM sent_brainrot_150m WHERE timestamp < datetime('now', '-12 hours')")
            deleted = conn.total_changes
            conn.commit()
            conn.close()
            if deleted > 0:
                print(f"🗑️ Banco limpo: {deleted} entradas")
                
        except Exception as e:
            print(f"Erro na limpeza: {e}")
        
        time.sleep(300)  # A cada 5 minutos

if __name__ == '__main__':
    print("🚀 Iniciando API OTIMIZADA para velocidade...")
    init_db()
    
    # Iniciar limpeza em background
    threading.Thread(target=background_cleanup, daemon=True).start()
    
    print("✅ API pronta! Envios serão MUITO mais rápidos!")
    print("🔧 Otimizações aplicadas:")
    print("   • Cache em memória para verificações instantâneas")
    print("   • Thread pool para envios paralelos")
    print("   • Resposta imediata ao Roblox")
    print("   • Timeouts curtos (5s) para Discord")
    print("   • Processamento em background")
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
